import { tool, type StructuredToolInterface } from "@langchain/core/tools";
import { zodToJsonSchema } from "zod-to-json-schema";
import type * as z from "zod/v3";

import type { BloombergToolName } from "./_defs_gen";
import {
  createToolResult,
  throwWithToolContext,
  type ToolContentAndArtifact,
  type ToolResultLimitOptions,
} from "./result-envelope";

type ZodOutput<T> = z.ZodType<T, z.ZodTypeDef, unknown>;

/**
 * Subset of the LangChain runnable config forwarded to tool functions.
 * `signal` aborts the call: the LangChain wrapper rejects immediately, and
 * Bloomberg tool functions use it to stop waiting and release subscriptions.
 */
export interface ToolInvocationConfig {
  readonly signal?: AbortSignal;
}

export interface BloombergToolResult {
  readonly value: unknown;
  /** Work already spent materializing Arrow rows before result traversal. */
  readonly materializedNodes?: number;
}

interface BloombergStructuredToolFields<Input> {
  readonly description: string;
  readonly name: BloombergToolName;
  readonly limits: ToolResultLimitOptions;
  readonly schema: ZodOutput<Input>;
}

const INPUT_JSON_SCHEMA_CACHE = new WeakMap<object, Record<string, unknown>>();

function freezeJsonSchema(root: Record<string, unknown>): Record<string, unknown> {
  const pending: object[] = [root];
  const seen = new WeakSet<object>();
  while (pending.length > 0) {
    const value = pending.pop();
    if (value === undefined || seen.has(value)) {
      continue;
    }
    seen.add(value);
    for (const rawChild of Object.values(value)) {
      const child: unknown = rawChild;
      if (typeof child === "object" && child !== null) {
        pending.push(child);
      }
    }
    Object.freeze(value);
  }
  return root;
}

function cloneJsonSchemaValue(
  value: unknown,
  seen: WeakMap<object, unknown>,
  active: WeakSet<object>,
): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError("JSON Schema numbers must be finite");
    }
    return value;
  }
  if (typeof value !== "object") {
    throw new TypeError(`JSON Schema contains unsupported ${typeof value} value`);
  }
  const cached = seen.get(value);
  if (cached !== undefined) {
    if (active.has(value)) {
      throw new TypeError("JSON Schema must not contain object cycles");
    }
    return cached;
  }
  const output: unknown[] | Record<string, unknown> = Array.isArray(value) ? [] : {};
  seen.set(value, output);
  active.add(value);
  try {
    for (const key of Object.keys(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || !("value" in descriptor)) {
        throw new TypeError(`JSON Schema property ${key} must not be an accessor`);
      }
      Object.defineProperty(output, key, {
        configurable: true,
        enumerable: true,
        value: cloneJsonSchemaValue(descriptor.value, seen, active),
        writable: true,
      });
    }
  } finally {
    active.delete(value);
  }
  return output;
}

function cachedRawJsonSchema(schema: Record<string, unknown>): Record<string, unknown> {
  const cached = INPUT_JSON_SCHEMA_CACHE.get(schema);
  if (cached !== undefined) {
    return cached;
  }
  const clone = cloneJsonSchemaValue(schema, new WeakMap<object, unknown>(), new WeakSet<object>());
  const immutable = freezeJsonSchema(clone as Record<string, unknown>);
  INPUT_JSON_SCHEMA_CACHE.set(schema, immutable);
  return immutable;
}

function inputJsonSchema(schema: ZodOutput<unknown>): Record<string, unknown> {
  const cached = INPUT_JSON_SCHEMA_CACHE.get(schema);
  if (cached !== undefined) {
    return cached;
  }
  const jsonSchema = zodToJsonSchema(schema, {
    $refStrategy: "none",
    effectStrategy: "input",
    pipeStrategy: "input",
  }) as Record<string, unknown>;
  delete jsonSchema.$schema;
  delete jsonSchema.definitions;
  const immutable = freezeJsonSchema(jsonSchema);
  INPUT_JSON_SCHEMA_CACHE.set(schema, immutable);
  return immutable;
}

/**
 * Provider-ready JSON Schema for a Bloomberg tool's input parameters, using
 * the same conversion settings as the embedded provider tool definition
 * ($ref-free, input-side of transforms). Exposed so consumers do not each
 * reinvent zod -> JSON Schema conversion and sanitization.
 */
export function toolParameterJsonSchema(
  toolInstance: StructuredToolInterface,
): Record<string, unknown> {
  const schema: unknown = toolInstance.schema;
  if (schema !== null && typeof schema === "object" && !("safeParse" in schema)) {
    return cachedRawJsonSchema(schema as Record<string, unknown>);
  }
  return inputJsonSchema(schema as ZodOutput<unknown>);
}

export function createBloombergStructuredTool<Input>(
  func: (
    input: Input,
    config?: ToolInvocationConfig,
  ) => BloombergToolResult | Promise<BloombergToolResult>,
  fields: BloombergStructuredToolFields<Input>,
): StructuredToolInterface {
  const providerToolDefinition = {
    type: "function",
    function: {
      description: fields.description,
      name: fields.name,
      parameters: inputJsonSchema(fields.schema),
    },
  };

  const guarded = async (
    input: Input,
    config?: ToolInvocationConfig,
  ): Promise<ToolContentAndArtifact> => {
    try {
      // Refuse to start Bloomberg work for calls that are already cancelled.
      config?.signal?.throwIfAborted();
      const result = await func(input, config);
      return createToolResult(
        fields.name,
        result.value,
        result.materializedNodes === undefined
          ? fields.limits
          : { ...fields.limits, materializedNodes: result.materializedNodes },
      );
    } catch (error) {
      throwWithToolContext(fields.name, error);
    }
  };

  return tool(
    guarded as never,
    {
      description: fields.description,
      name: fields.name,
      responseFormat: "content_and_artifact",
      schema: fields.schema,
      extras: { providerToolDefinition },
    } as never,
  ) as StructuredToolInterface;
}
