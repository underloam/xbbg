//! Schema introspector - converts xbbg_core FFI types to serde-enabled types.
//!
//! This module bridges the gap between xbbg_core's raw FFI schema wrappers
//! and the serde-enabled types used for JSON serialization.

use xbbg_core::DataType;
use xbbg_core::schema::{Operation, SchemaElementDefinition, SchemaTypeDefinition};
use xbbg_core::service::Service;

use super::types::{ElementInfo, OperationSchema, ServiceSchema};

/// Convert a Bloomberg DataType to a string representation.
fn datatype_to_string(dt: DataType) -> String {
    match dt {
        DataType::Bool => "Bool".to_string(),
        DataType::Char => "Char".to_string(),
        DataType::Byte => "Byte".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::Float32 => "Float32".to_string(),
        DataType::Float64 => "Float64".to_string(),
        DataType::String => "String".to_string(),
        DataType::ByteArray => "ByteArray".to_string(),
        DataType::Date => "Date".to_string(),
        DataType::Time => "Time".to_string(),
        DataType::Decimal => "Decimal".to_string(),
        DataType::Datetime => "Datetime".to_string(),
        DataType::Enumeration => "Enumeration".to_string(),
        DataType::Sequence => "Sequence".to_string(),
        DataType::Choice => "Choice".to_string(),
        DataType::CorrelationId => "CorrelationId".to_string(),
    }
}

/// Extract enum values from a SchemaTypeDefinition if it's an enumeration.
fn extract_enum_values(type_def: &SchemaTypeDefinition<'_>) -> Option<Vec<String>> {
    if !type_def.is_enumeration_type() {
        return None;
    }

    type_def
        .enumeration()
        .map(|constants| constants.iter().map(|c| c.name_str().to_string()).collect())
}

const MAX_SCHEMA_NESTING_DEPTH: usize = 32;

fn complex_type_cycle_key(data_type: DataType, type_name: &str) -> Option<String> {
    if type_name.is_empty() {
        None
    } else {
        Some(format!("{}:{type_name}", datatype_to_string(data_type)))
    }
}

fn can_descend_complex_type(
    is_complex: bool,
    data_type: DataType,
    type_name: &str,
    depth: usize,
    active_complex_types: &[String],
) -> bool {
    if !is_complex || depth >= MAX_SCHEMA_NESTING_DEPTH {
        return false;
    }

    let Some(key) = complex_type_cycle_key(data_type, type_name) else {
        // Anonymous complex types cannot be cycle-detected reliably by name;
        // the depth cap still prevents unbounded expansion.
        return true;
    };

    !active_complex_types.iter().any(|active| active == &key)
}

/// Convert a SchemaElementDefinition to ElementInfo.
///
/// This recursively converts child elements for complex types.
fn convert_element_def(elem_def: &SchemaElementDefinition<'_>) -> ElementInfo {
    let mut active_complex_types = Vec::new();
    convert_element_def_inner(elem_def, 0, &mut active_complex_types)
}

fn convert_element_def_inner(
    elem_def: &SchemaElementDefinition<'_>,
    depth: usize,
    active_complex_types: &mut Vec<String>,
) -> ElementInfo {
    let type_def = elem_def.type_definition();
    let data_type = type_def.datatype();
    let type_name = type_def.name_str().to_string();

    // Get children for complex types, guarding recursive Bloomberg schemas such as BQL.
    let children: Vec<ElementInfo> = if can_descend_complex_type(
        type_def.is_complex_type(),
        data_type,
        &type_name,
        depth,
        active_complex_types,
    ) {
        let cycle_key = complex_type_cycle_key(data_type, &type_name);
        if let Some(key) = cycle_key.as_ref() {
            active_complex_types.push(key.clone());
        }

        let converted = type_def
            .element_definitions()
            .map(|child_def| convert_element_def_inner(&child_def, depth + 1, active_complex_types))
            .collect();

        if cycle_key.is_some() {
            active_complex_types.pop();
        }

        converted
    } else {
        Vec::new()
    };

    // Get enum values if this is an enumeration type
    let enum_values = extract_enum_values(&type_def);

    ElementInfo {
        name: elem_def.name_str().to_string(),
        description: elem_def.description().to_string(),
        data_type: datatype_to_string(data_type),
        type_name,
        is_array: elem_def.is_array(),
        is_optional: elem_def.is_optional(),
        enum_values,
        children,
    }
}
/// Convert a Bloomberg Operation to OperationSchema.
pub(crate) fn convert_operation(op: &Operation<'_>) -> OperationSchema {
    // Convert request definition
    let request = match op.request_definition() {
        Ok(req_def) => convert_element_def(&req_def),
        Err(e) => {
            xbbg_log::warn!(op = op.name(), error = %e, "Failed to get request definition");
            ElementInfo::empty()
        }
    };

    // Convert response definitions
    let responses: Vec<ElementInfo> = (0..op.num_response_definitions())
        .filter_map(|i| {
            op.response_definition(i)
                .ok()
                .map(|resp_def| convert_element_def(&resp_def))
        })
        .collect();

    OperationSchema {
        name: op.name().to_string(),
        description: op.description().to_string(),
        request,
        responses,
    }
}

/// Introspect a Bloomberg Service and convert to ServiceSchema.
///
/// This iterates over all operations in the service and converts them
/// to serde-enabled types suitable for JSON serialization.
pub fn introspect_service(service: &Service<'_>, service_uri: &str) -> ServiceSchema {
    let operations: Vec<OperationSchema> = service
        .operations()
        .map(|op| convert_operation(&op))
        .collect();

    ServiceSchema::new(
        service_uri.to_string(),
        service.description().to_string(),
        operations,
    )
}

/// Find an operation by name and convert to OperationSchema.
pub fn introspect_operation(
    service: &Service<'_>,
    operation_name: &str,
) -> Option<OperationSchema> {
    service
        .operations()
        .find(|op| op.name() == operation_name)
        .map(|op| convert_operation(&op))
}

/// List all operation names for a service.
pub fn list_operation_names(service: &Service<'_>) -> Vec<String> {
    service
        .operations()
        .map(|op| op.name().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_to_string() {
        assert_eq!(datatype_to_string(DataType::Int32), "Int32");
        assert_eq!(datatype_to_string(DataType::String), "String");
        assert_eq!(datatype_to_string(DataType::Sequence), "Sequence");
        assert_eq!(datatype_to_string(DataType::Enumeration), "Enumeration");
    }

    #[test]
    fn test_element_info_empty() {
        let elem = ElementInfo::empty();
        assert!(elem.name.is_empty());
        assert!(elem.children.is_empty());
        assert!(elem.enum_values.is_none());
    }

    #[test]
    fn test_complex_type_cycle_key_uses_data_type_and_name() {
        assert_eq!(
            complex_type_cycle_key(DataType::Sequence, "ClientContextType"),
            Some("Sequence:ClientContextType".to_string())
        );
        assert_eq!(complex_type_cycle_key(DataType::Sequence, ""), None);
    }

    #[test]
    fn test_can_descend_complex_type_rejects_active_cycle() {
        let active = vec!["Sequence:RecursiveType".to_string()];

        assert!(!can_descend_complex_type(
            true,
            DataType::Sequence,
            "RecursiveType",
            1,
            &active,
        ));
    }

    #[test]
    fn test_can_descend_complex_type_rejects_max_depth() {
        let active = Vec::new();

        assert!(!can_descend_complex_type(
            true,
            DataType::Sequence,
            "DeepType",
            MAX_SCHEMA_NESTING_DEPTH,
            &active,
        ));
    }

    #[test]
    fn test_can_descend_complex_type_allows_non_recursive_named_type() {
        let active = vec!["Sequence:ParentType".to_string()];

        assert!(can_descend_complex_type(
            true,
            DataType::Sequence,
            "ChildType",
            1,
            &active,
        ));
    }
}
