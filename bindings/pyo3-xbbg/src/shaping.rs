//! Result shaping for generated endpoints, off the asyncio thread.
//!
//! BQR quote shaping and BDH presentation run on whole request results after
//! the middleware chain. The `a`-prefixed functions here share a native Arrow
//! carrier's Arc-backed batches while attached and return an awaitable. The
//! kernel then runs on the bridge runtime's blocking pool: not on the asyncio
//! thread, not on a bridge worker, and never attached. The new `ArrowTable` is
//! published in the completion attach (see `completion.rs`).

use arrow_array::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_arrow::TableData;
use xbbg_recipes::RecipeError;
use xbbg_recipes::fixed_income::{reshape_bqr_generic, shape_bqr_quotes};

use crate::completion::shutdown_safe_future;
use crate::native_arrow::{ArrowRecordBatch, ArrowTable, core_err_to_py, historical_presentation};

/// Arrow data shared from an xbbg carrier, owned by a shaping kernel.
enum NativeInput {
    Table(TableData),
    Batch(RecordBatch),
}

impl NativeInput {
    /// Share a carrier's batches: Arc clones only, no buffer copies.
    fn extract(data: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(table) = data.cast::<ArrowTable>() {
            return Ok(Self::Table(table.get().table_data().clone()));
        }
        if let Ok(batch) = data.cast::<ArrowRecordBatch>() {
            return Ok(Self::Batch(batch.get().batch().clone()));
        }
        Err(PyTypeError::new_err(format!(
            "Expected xbbg ArrowTable or ArrowRecordBatch, got {}",
            data.get_type().name()?
        )))
    }

    fn into_batch(self) -> PyResult<RecordBatch> {
        match self {
            Self::Table(table) => table.combined_batch().map_err(core_err_to_py),
            Self::Batch(batch) => Ok(batch),
        }
    }

    fn into_table(self) -> PyResult<TableData> {
        match self {
            Self::Table(table) => Ok(table),
            Self::Batch(batch) => TableData::try_new(vec![batch]).map_err(core_err_to_py),
        }
    }
}

/// Map shared BQR shaper failures: missing broker attribution is a
/// `RuntimeError` carrying the shaper's message; Arrow failures are `ValueError`s.
fn shaping_err(err: RecipeError) -> PyErr {
    match err {
        RecipeError::Other(message) => PyRuntimeError::new_err(message),
        other => PyValueError::new_err(other.to_string()),
    }
}

/// Shape raw BQR output with the shared recipe shaper.
fn shape_bqr_input(
    input: NativeInput,
    ticker: &str,
    key_column: &str,
    enforce_broker_codes: bool,
) -> PyResult<TableData> {
    let shaped = shape_bqr_quotes(
        input.into_batch()?,
        ticker,
        key_column,
        enforce_broker_codes,
    )
    .map_err(shaping_err)?;
    TableData::try_new(vec![shaped]).map_err(core_err_to_py)
}

/// Pivot generic BQR rows with the shared recipe reshape.
///
/// Pure Arrow work: `ArrowTable.reshape_bqr_generic` runs it detached.
pub(crate) fn reshape_bqr_generic_table(
    data: &TableData,
    ticker: &str,
    key_column: &str,
) -> PyResult<TableData> {
    let batch = data.combined_batch().map_err(core_err_to_py)?;
    let reshaped = reshape_bqr_generic(&batch, ticker, key_column).map_err(shaping_err)?;
    TableData::try_new(vec![reshaped]).map_err(core_err_to_py)
}

/// Run a shaping kernel on the bridge runtime's blocking pool.
///
/// The awaiting future only polls the join handle, so neither the asyncio
/// thread nor a bridge worker runs the kernel, and nothing attaches. A kernel
/// panic resumes in the polled future, which pyo3-async-runtimes raises as
/// `RustPanic`. Cancelling the awaitable cannot stop a started kernel; its
/// owned result is then dropped on the blocking thread.
async fn run_blocking<T, F>(kernel: F) -> PyResult<T>
where
    F: FnOnce() -> PyResult<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(kernel).await {
        Ok(result) => result,
        Err(error) => match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_) => Err(PyRuntimeError::new_err(
                "shaping task was cancelled before it ran",
            )),
        },
    }
}

/// Shape raw BQR IntradayTick output into quote rows, off the event loop.
///
/// Uses the BQR recipe's shaper, shared with `xbbg.ext.abqr` and Node: generic
/// `path` output is pivoted into one row per tick with the security in
/// `key_column` (fields holding text are strings; other fields keep the
/// `value_num` type, Float64 from the generic extractor), attributed requests
/// require a broker code, rows are sorted by `time` with nulls last, and
/// Bloomberg names become `event_type`, `price`, `broker_buy`, `broker_sell`,
/// `spread_price`, `condition_codes` and `exchange`. Schema metadata is kept.
///
/// Args:
///     data: Raw result as an xbbg ArrowTable or ArrowRecordBatch.
///     ticker: Requested security, the key of generic output rows.
///     key_column: Name of the security column, the engine's key column.
///     enforce_broker_codes: Raise RuntimeError when quote rows carry no broker code.
///
/// Returns:
///     An awaitable resolving to an ArrowTable.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (data, ticker, key_column, enforce_broker_codes=true))]
fn ashape_bqr<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
    ticker: String,
    key_column: String,
    enforce_broker_codes: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let input = NativeInput::extract(data)?;
    shutdown_safe_future(py, async move {
        run_blocking(move || shape_bqr_input(input, &ticker, &key_column, enforce_broker_codes))
            .await
            .map(ArrowTable::from_data)
    })
}

/// Apply BDH presentation options off the event loop.
///
/// Same result as `ArrowTable.apply_historical_presentation`: `sort` orders
/// by `key_column`, date (descending only for "DESCENDING") and field, nulls
/// last; `date_format` "PERIODIC" replaces `date` with a UTF-8 `period` label
/// and "BOTH" inserts it after `date`; `show_date=False` drops both.
///
/// Args:
///     data: Historical result as an xbbg ArrowTable or ArrowRecordBatch.
///     show_date: False drops the date and period columns.
///     date_format: "PERIODIC" or "BOTH" adds period labels.
///     sort: Sort rows; "DESCENDING" orders dates newest first.
///     periodicity: Label granularity, such as "MONTHLY" (default daily).
///     key_column: Name of the security column, the engine's key column.
///
/// Returns:
///     An awaitable resolving to an ArrowTable.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(signature = (data, show_date, date_format, sort, periodicity, key_column))]
fn apresent_historical<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
    show_date: Option<bool>,
    date_format: Option<String>,
    sort: Option<String>,
    periodicity: Option<String>,
    key_column: String,
) -> PyResult<Bound<'py, PyAny>> {
    let input = NativeInput::extract(data)?;
    shutdown_safe_future(py, async move {
        run_blocking(move || {
            let table = input.into_table()?;
            historical_presentation(
                &table,
                show_date,
                date_format.as_deref(),
                sort.as_deref(),
                periodicity.as_deref(),
                &key_column,
            )
            .map_err(core_err_to_py)
        })
        .await
        .map(ArrowTable::from_data)
    })
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ashape_bqr, m)?)?;
    m.add_function(wrap_pyfunction!(apresent_historical, m)?)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn synthetic_metadata() -> HashMap<String, String> {
        HashMap::from([("xbbg.eid_data".to_string(), "{}".to_string())])
    }

    fn column(name: &str, array: impl Array + 'static) -> (&str, ArrayRef) {
        (name, Arc::new(array))
    }

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let schema = Schema::new_with_metadata(fields, synthetic_metadata());
        let arrays = columns.into_iter().map(|(_, array)| array).collect();
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    fn typed_quotes(times: Vec<Option<&str>>, sellers: Vec<Option<&str>>) -> RecordBatch {
        let rows = times.len();
        batch(vec![
            column("ticker", StringArray::from(vec!["SYN1 Corp"; rows])),
            column("time", StringArray::from(times)),
            column("type", StringArray::from(vec!["BID"; rows])),
            column(
                "value",
                Float64Array::from_iter_values((0..rows).map(|row| row as f64)),
            ),
            column(
                "size",
                Int64Array::from_iter_values((0..rows).map(|row| row as i64)),
            ),
            column("brokerSellCode", StringArray::from(sellers)),
        ])
    }

    fn column_names(table: &TableData) -> Vec<String> {
        table.column_names()
    }

    fn texts<'a>(table: &'a TableData, name: &str) -> Vec<Option<&'a str>> {
        let [batch] = table.batches.as_slice() else {
            panic!("shaped tables have one batch");
        };
        let column = batch.column_by_name(name).unwrap();
        let values = column.as_any().downcast_ref::<StringArray>().unwrap();
        values.iter().collect()
    }

    #[test]
    fn bqr_input_from_a_multi_batch_table_is_shaped_once_and_keeps_metadata() {
        let first = typed_quotes(
            vec![Some("2024-01-15T09:00:02"), None],
            vec![Some("DLRB"), Some("DLRN")],
        );
        let second = typed_quotes(vec![Some("2024-01-15T09:00:01")], vec![Some("DLRA")]);
        let table = TableData::try_new(vec![first, second]).unwrap();

        let shaped =
            shape_bqr_input(NativeInput::Table(table), "SYN1 Corp", "ticker", true).unwrap();

        assert_eq!(shaped.batches.len(), 1);
        assert_eq!(
            column_names(&shaped),
            [
                "ticker",
                "time",
                "event_type",
                "price",
                "size",
                "broker_sell"
            ]
        );
        assert_eq!(
            texts(&shaped, "broker_sell"),
            [Some("DLRA"), Some("DLRB"), Some("DLRN")]
        );
        assert_eq!(
            texts(&shaped, "time"),
            [
                Some("2024-01-15T09:00:01"),
                Some("2024-01-15T09:00:02"),
                None
            ]
        );
        assert_eq!(
            shaped.schema.field_with_name("size").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(shaped.schema.metadata(), &synthetic_metadata());
    }

    #[test]
    fn bqr_failures_keep_their_python_exception_types() {
        Python::initialize();
        let unattributed = typed_quotes(vec![Some("2024-01-15T09:00:00")], vec![Some("")]);

        let error = shape_bqr_input(
            NativeInput::Batch(unattributed.clone()),
            "SYN1 Corp",
            "ticker",
            true,
        )
        .unwrap_err();
        let unenforced = shape_bqr_input(
            NativeInput::Batch(unattributed),
            "SYN1 Corp",
            "ticker",
            false,
        );
        assert!(unenforced.is_ok());

        Python::attach(|py| {
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert_eq!(
                error.value(py).to_string(),
                "BQR returned quote rows without broker attribution. Use a fixed-income \
                 ticker with a dealer quote pricing source such as '@MSG1 Corp', or pass \
                 include_broker_codes=False if raw quote ticks without dealer codes are \
                 intentional."
            );
            let arrow = shaping_err(RecipeError::Arrow(arrow_schema::ArrowError::ComputeError(
                "synthetic".into(),
            )));
            assert!(arrow.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn kernels_run_on_the_blocking_pool_while_the_caller_keeps_the_interpreter() {
        Python::initialize();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        Python::attach(|_py| {
            let caller = std::thread::current().id();
            // On GIL builds the caller holds the interpreter for the whole
            // wait, so a kernel that attached could not finish.
            let (kernel_thread, kernel_attached) = runtime
                .block_on(run_blocking(|| {
                    Ok((std::thread::current().id(), unsafe {
                        pyo3::ffi::PyGILState_Check()
                    }))
                }))
                .unwrap();
            assert_ne!(kernel_thread, caller);
            assert_eq!(kernel_attached, 0);
        });
    }

    #[test]
    fn kernel_panics_resume_in_the_awaiting_future() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let joined = runtime.block_on(async {
            tokio::spawn(run_blocking::<(), _>(|| panic!("synthetic shaping panic"))).await
        });
        let panic = joined.unwrap_err().into_panic();
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"synthetic shaping panic")
        );
    }
}
