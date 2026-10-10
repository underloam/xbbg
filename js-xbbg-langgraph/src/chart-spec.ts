import {
  CHART_DEFAULTS,
  CLOSE_FIELD_CANDIDATES,
  HIGH_FIELD_CANDIDATES,
  LABEL_FIELD_CANDIDATES,
  LOW_FIELD_CANDIDATES,
  OPEN_FIELD_CANDIDATES,
  PRICE_FIELD_CANDIDATES,
  SERIES_FIELD_CANDIDATES,
  SIDE_FIELD_CANDIDATES,
  SIZE_FIELD_CANDIDATES,
  VALUE_FIELD_CANDIDATES,
  X_FIELD_CANDIDATES,
} from "./_defs_gen";
import type { BloombergChartSource, ChartKind, ChartRow, ChartSpecInput } from "./ext-schemas";

type GenericChartKind = Exclude<ChartKind, "bar" | "candlestick" | "depth">;
type VegaType = "nominal" | "ordinal" | "quantitative" | "temporal";
type VegaEncoding = Record<string, unknown>;

export interface ChartSpecSummary {
  readonly chart: ChartKind;
  readonly inputRows: number;
  readonly renderer: "vega-lite";
  readonly rowCount: number;
  readonly source: BloombergChartSource;
  readonly title: string;
  readonly truncatedInput: boolean;
  readonly xField?: string;
  readonly yFields: readonly string[];
  readonly seriesField?: string;
}

export interface VegaLiteSpec {
  readonly $schema: "https://vega.github.io/schema/vega-lite/v5.json";
  readonly data: { readonly values: readonly ChartRow[] };
  readonly description: string;
  readonly title: string;
  readonly [key: string]: unknown;
}

export interface ChartSpecOutput {
  readonly kind: "xbbg.visualization";
  readonly version: 1;
  readonly component: "xbbg_chart";
  readonly renderer: "vega-lite";
  readonly rowCount: number;
  readonly inputRowCount: number;
  readonly truncatedInput: boolean;
  readonly source: BloombergChartSource;
  readonly chart: ChartKind;
  readonly summary: ChartSpecSummary;
  readonly spec: VegaLiteSpec;
  readonly warnings: readonly string[];
}

interface BuiltChartSpec {
  readonly spec: VegaLiteSpec;
  readonly xField: string;
  readonly yFields: readonly string[];
  readonly seriesField?: string;
}

const VEGA_SCHEMA = "https://vega.github.io/schema/vega-lite/v5.json" as const;
const COMPONENT_NAME = "xbbg_chart" as const;

function fieldExists(rows: readonly ChartRow[], field: string): boolean {
  for (const row of rows) {
    if (Object.prototype.hasOwnProperty.call(row, field)) {
      return true;
    }
  }
  return false;
}

function findCandidateField(
  rows: readonly ChartRow[],
  candidates: readonly string[],
): string | undefined {
  for (const candidate of candidates) {
    if (fieldExists(rows, candidate)) {
      return candidate;
    }
  }

  const first = rows[0];
  if (first === undefined) {
    return undefined;
  }
  const keys = Object.keys(first);
  for (const candidate of candidates) {
    const lower = candidate.toLowerCase();
    const match = keys.find((key) => key.toLowerCase() === lower);
    if (match !== undefined && fieldExists(rows, match)) {
      return match;
    }
  }
  return undefined;
}

function requireField(
  rows: readonly ChartRow[],
  field: string | undefined,
  label: string,
  candidates: readonly string[],
): string {
  const resolved = field ?? findCandidateField(rows, candidates);
  if (resolved === undefined || !fieldExists(rows, resolved)) {
    throw new Error(
      `Missing ${label}; pass ${label} explicitly or include one of: ${candidates.join(", ")}`,
    );
  }
  return resolved;
}

function hasFiniteNumber(rows: readonly ChartRow[], field: string): boolean {
  for (const row of rows) {
    if (typeof row[field] === "number" && Number.isFinite(row[field])) {
      return true;
    }
  }
  return false;
}

function firstNumericField(rows: readonly ChartRow[], excludedField: string): string | undefined {
  const first = rows[0];
  if (first === undefined) {
    return undefined;
  }
  for (const key of Object.keys(first)) {
    if (key !== excludedField && hasFiniteNumber(rows, key)) {
      return key;
    }
  }
  return undefined;
}

function requireNumericField(rows: readonly ChartRow[], field: string, label: string): void {
  if (!hasFiniteNumber(rows, field)) {
    throw new Error(`${label} (${field}) must contain at least one finite numeric value`);
  }
}

function inferVegaType(rows: readonly ChartRow[], field: string): VegaType {
  for (const row of rows) {
    const value = row[field];
    if (typeof value === "number") {
      return "quantitative";
    }
    if (
      typeof value === "string" &&
      (/^\d{4}-\d{2}-\d{2}(?:$|[T\s])/u.test(value) || /^\d{8}$/u.test(value))
    ) {
      return "temporal";
    }
  }
  return "nominal";
}

function normalizeTemporalRows(rows: readonly ChartRow[], field: string): readonly ChartRow[] {
  let normalized: ChartRow[] | undefined;
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    if (row === undefined) {
      continue;
    }
    const value = row[field];
    if (typeof value !== "string" || !/^\d{8}$/u.test(value)) {
      normalized?.push(row);
      continue;
    }
    normalized ??= rows.slice(0, index);
    normalized.push({
      ...row,
      [field]: `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`,
    });
  }
  return normalized ?? rows;
}

function tooltip(fields: readonly string[]): readonly VegaEncoding[] {
  return fields.map((field) => ({
    field,
    type: field === "_xbbg_value" ? "quantitative" : "nominal",
  }));
}

function datumField(field: string): string {
  return `datum[${JSON.stringify(field)}]`;
}

function buildGenericSpec(
  input: ChartSpecInput,
  rows: readonly ChartRow[],
  chart: GenericChartKind,
  title: string,
): BuiltChartSpec {
  const xField = requireField(rows, input.xField, "xField", X_FIELD_CANDIDATES);
  let yFields: readonly string[];
  if (input.yFields !== undefined) {
    yFields = input.yFields;
  } else {
    const yField =
      findCandidateField(rows, VALUE_FIELD_CANDIDATES) ?? firstNumericField(rows, xField);
    if (yField === undefined) {
      throw new Error("Missing yFields; include at least one numeric value field");
    }
    yFields = [yField];
  }
  if (yFields.length === 0) {
    throw new Error("Missing yFields; include at least one numeric value field");
  }
  for (const field of yFields) {
    if (!fieldExists(rows, field)) {
      throw new Error(`Missing y field: ${field}`);
    }
    requireNumericField(rows, field, "yField");
  }

  const normalizedRows =
    inferVegaType(rows, xField) === "temporal" ? normalizeTemporalRows(rows, xField) : rows;
  const seriesField =
    input.seriesField ??
    (yFields.length === 1 ? findCandidateField(rows, SERIES_FIELD_CANDIDATES) : undefined);
  if (seriesField !== undefined && !fieldExists(rows, seriesField)) {
    throw new Error(`Missing series field: ${seriesField}`);
  }

  const mark = chart === "scatter" ? "point" : chart;
  const encoding: VegaEncoding = {
    x: { field: xField, title: xField, type: inferVegaType(normalizedRows, xField) },
  };

  const transform: unknown[] = [];
  if (yFields.length === 1) {
    const yField = yFields[0];
    if (yField === undefined) {
      throw new Error("Missing yFields; include at least one numeric value field");
    }
    encoding.y = { field: yField, title: yField, type: "quantitative" };
    if (seriesField !== undefined) {
      encoding.color = { field: seriesField, title: seriesField, type: "nominal" };
    }
    encoding.tooltip = tooltip([
      xField,
      ...(seriesField === undefined ? [] : [seriesField]),
      yField,
    ]);
  } else {
    transform.push({ as: ["_xbbg_series", "_xbbg_value"], fold: yFields });
    encoding.y = { field: "_xbbg_value", title: "value", type: "quantitative" };
    encoding.color = { field: "_xbbg_series", title: "series", type: "nominal" };
    if (seriesField !== undefined) {
      encoding.detail = { field: seriesField, type: "nominal" };
    }
    encoding.tooltip = tooltip([
      xField,
      ...(seriesField === undefined ? [] : [seriesField]),
      "_xbbg_series",
      "_xbbg_value",
    ]);
  }

  const spec: VegaLiteSpec = {
    $schema: VEGA_SCHEMA,
    data: { values: normalizedRows },
    description: `xbbg ${chart} chart spec for ${input.source}`,
    mark: { type: mark, tooltip: true },
    title,
    ...(transform.length === 0 ? {} : { transform }),
    encoding,
  };
  return { spec, xField, yFields, ...(seriesField === undefined ? {} : { seriesField }) };
}

function buildBarSpec(
  input: ChartSpecInput,
  rows: readonly ChartRow[],
  title: string,
): BuiltChartSpec {
  const xField = requireField(
    rows,
    input.xField ?? input.labelField,
    "labelField",
    LABEL_FIELD_CANDIDATES,
  );
  const yField = requireField(
    rows,
    input.valueField ?? input.yFields?.[0],
    "valueField",
    VALUE_FIELD_CANDIDATES,
  );
  requireNumericField(rows, yField, "valueField");
  const seriesField = input.seriesField;
  if (seriesField !== undefined && !fieldExists(rows, seriesField)) {
    throw new Error(`Missing series field: ${seriesField}`);
  }
  const encoding: VegaEncoding = {
    x: { field: xField, sort: "-y", title: xField, type: inferVegaType(rows, xField) },
    y: { field: yField, title: yField, type: "quantitative" },
    tooltip: tooltip([xField, ...(seriesField === undefined ? [] : [seriesField]), yField]),
  };
  if (seriesField !== undefined) {
    encoding.color = { field: seriesField, title: seriesField, type: "nominal" };
  }
  return {
    spec: {
      $schema: VEGA_SCHEMA,
      data: { values: rows },
      description: `xbbg bar chart spec for ${input.source}`,
      encoding,
      mark: { type: "bar", tooltip: true },
      title,
    },
    xField,
    yFields: [yField],
    ...(seriesField === undefined ? {} : { seriesField }),
  };
}

function buildCandlestickSpec(
  input: ChartSpecInput,
  rows: readonly ChartRow[],
  title: string,
): BuiltChartSpec {
  const xField = requireField(rows, input.xField, "xField", X_FIELD_CANDIDATES);
  const openField = requireField(rows, input.openField, "openField", OPEN_FIELD_CANDIDATES);
  const highField = requireField(rows, input.highField, "highField", HIGH_FIELD_CANDIDATES);
  const lowField = requireField(rows, input.lowField, "lowField", LOW_FIELD_CANDIDATES);
  const closeField = requireField(rows, input.closeField, "closeField", CLOSE_FIELD_CANDIDATES);
  for (const [label, field] of [
    ["openField", openField],
    ["highField", highField],
    ["lowField", lowField],
    ["closeField", closeField],
  ] as const) {
    requireNumericField(rows, field, label);
  }
  const normalizedRows =
    inferVegaType(rows, xField) === "temporal" ? normalizeTemporalRows(rows, xField) : rows;
  const color = {
    condition: { test: `${datumField(closeField)} >= ${datumField(openField)}`, value: "#137333" },
    value: "#c5221f",
  };
  return {
    spec: {
      $schema: VEGA_SCHEMA,
      data: { values: normalizedRows },
      description: `xbbg candlestick chart spec for ${input.source}`,
      encoding: {
        x: { field: xField, title: xField, type: inferVegaType(normalizedRows, xField) },
      },
      layer: [
        {
          mark: "rule",
          encoding: {
            color,
            tooltip: tooltip([xField, openField, highField, lowField, closeField]),
            y: { field: lowField, title: "price", type: "quantitative" },
            y2: { field: highField },
          },
        },
        {
          mark: "bar",
          encoding: {
            color,
            y: { field: openField, title: "price", type: "quantitative" },
            y2: { field: closeField },
          },
        },
      ],
      title,
    },
    xField,
    yFields: [openField, highField, lowField, closeField],
  };
}

function buildDepthSpec(
  input: ChartSpecInput,
  rows: readonly ChartRow[],
  title: string,
): BuiltChartSpec {
  const priceField = requireField(
    rows,
    input.priceField ?? input.xField,
    "priceField",
    PRICE_FIELD_CANDIDATES,
  );
  const sizeField = requireField(
    rows,
    input.sizeField ?? input.valueField ?? input.yFields?.[0],
    "sizeField",
    SIZE_FIELD_CANDIDATES,
  );
  const sideField = requireField(
    rows,
    input.sideField ?? input.seriesField,
    "sideField",
    SIDE_FIELD_CANDIDATES,
  );
  requireNumericField(rows, priceField, "priceField");
  requireNumericField(rows, sizeField, "sizeField");
  return {
    spec: {
      $schema: VEGA_SCHEMA,
      data: { values: rows },
      description: `xbbg market depth chart spec for ${input.source}`,
      encoding: {
        color: { field: sideField, title: sideField, type: "nominal" },
        tooltip: tooltip([sideField, priceField, sizeField]),
        x: { field: priceField, title: priceField, type: "quantitative" },
        y: { field: sizeField, title: sizeField, type: "quantitative" },
      },
      mark: { type: "bar", tooltip: true },
      title,
    },
    xField: priceField,
    yFields: [sizeField],
    seriesField: sideField,
  };
}

export function createChartSpec(input: ChartSpecInput): ChartSpecOutput {
  const maxPoints = input.maxPoints ?? input.rows.length;
  const rows = input.rows.length > maxPoints ? input.rows.slice(0, maxPoints) : input.rows;
  if (rows.length === 0) {
    throw new Error("rows must contain at least one chart data row");
  }

  const chart = input.chart ?? CHART_DEFAULTS[input.source];
  const title = input.title ?? `${input.source} ${chart}`;
  const warnings: string[] = [];
  if (rows.length !== input.rows.length) {
    warnings.push(
      `Chart spec contains first ${rows.length} of ${input.rows.length} rows; narrow the upstream request for a complete visualization.`,
    );
  }

  const built =
    chart === "candlestick"
      ? buildCandlestickSpec(input, rows, title)
      : chart === "depth"
        ? buildDepthSpec(input, rows, title)
        : chart === "bar"
          ? buildBarSpec(input, rows, title)
          : buildGenericSpec(input, rows, chart, title);

  const summary: ChartSpecSummary = {
    chart,
    inputRows: input.rows.length,
    renderer: "vega-lite",
    rowCount: rows.length,
    source: input.source,
    title,
    truncatedInput: rows.length !== input.rows.length,
    xField: built.xField,
    yFields: built.yFields,
    ...(built.seriesField === undefined ? {} : { seriesField: built.seriesField }),
  };

  return {
    kind: "xbbg.visualization",
    version: 1,
    component: COMPONENT_NAME,
    renderer: "vega-lite",
    rowCount: rows.length,
    inputRowCount: input.rows.length,
    truncatedInput: rows.length !== input.rows.length,
    source: input.source,
    chart,
    summary,
    spec: built.spec,
    warnings,
  };
}
