import { createCoreResolver } from "./core-loader";
import { createBloombergExtToolsForResolver } from "./ext-tools";
import type { BloombergToolsOptions } from "./options";
import { createBloombergToolsForResolver, type BloombergTool } from "./tools";

export {
  BLOOMBERG_TOOL_NAMES,
  type BloombergToolName,
  type ResultTruncationReason,
} from "./_defs_gen";

export {
  BLOOMBERG_TOOL_INSTRUCTIONS,
  getBloombergToolInstructions,
  type BloombergToolInstructionsOptions,
} from "./descriptions";
export {
  BLOOMBERG_EXT_TOOL_NAMES,
  createBloombergExtTools,
  createExtBqlBuilderTool,
  createExtCalculateTool,
  createExtChartSpecTool,
  createExtCdxTool,
  createExtColumnsTool,
  createExtConstantsTool,
  createExtCurrencyTool,
  createExtFuturesTool,
  createExtMarketSessionTool,
  createExtTickerTool,
  createExtYasOverridesTool,
} from "./ext-tools";
export type { ToolInvocationConfig } from "./langchain-tool";
export { toolParameterJsonSchema } from "./langchain-tool";
export {
  DEFAULT_ENGINE_REQUEST_TIMEOUT_MS,
  type BloombergToolsOptions,
  type NormalizedBloombergToolsOptions,
} from "./options";
export type { ToolEnvelope } from "./result-envelope";
export type { ResultTruncationSummary } from "./result-types";
export type { ChartSpecOutput, ChartSpecSummary, VegaLiteSpec } from "./chart-spec";
export type {
  BloombergChartSource,
  ChartKind,
  ChartRenderer,
  ChartRow,
  ChartScalar,
  ChartSpecInput,
} from "./ext-schemas";
export {
  createAuctionSnapshotTool,
  createBdhTool,
  createBdibTool,
  createBeqsTool,
  createBdtickTool,
  createBdpTool,
  createBdsTool,
  createCheckEntitlementsTool,
  createBfldsTool,
  createCorporateBondsTool,
  createEtfHoldingsTool,
  createDepthSnapshotTool,
  createIndexMembersTool,
  createIssuerIsinsTool,
  createMktbarSnapshotTool,
  createPreferredsTool,
  createResolveIsinsTool,
  createResolveVenuesTool,
  createStreamSnapshotTool,
  createYasTool,
  createBloombergTools,
  createBqlTool,
  createBsrchTool,
  createBqrTool,
  type BloombergTool,
} from "./tools";

export function createAllBloombergTools(options: BloombergToolsOptions = {}): BloombergTool[] {
  const resolver = createCoreResolver(options);
  return [
    ...createBloombergToolsForResolver(resolver),
    ...createBloombergExtToolsForResolver(resolver),
  ];
}
