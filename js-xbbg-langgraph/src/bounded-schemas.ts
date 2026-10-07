import * as z from "zod/v3";

import type { NormalizedBloombergToolsOptions } from "./options";

export type ZodOutput<T> = z.ZodType<T, z.ZodTypeDef, unknown>;
type PrimitiveValue = string | number | boolean;
export type PrimitiveMap = Record<string, PrimitiveValue>;
export type OverrideMap = Record<string, PrimitiveValue | PrimitiveMap>;

interface LengthBounds {
  readonly minimum?: number;
  readonly minimumMessage?: string;
  readonly maximumMessage?: string;
}

export function boundedString(maxChars: number, bounds: LengthBounds = {}): z.ZodString {
  return z
    .string()
    .trim()
    .min(bounds.minimum ?? 1, bounds.minimumMessage)
    .max(maxChars, bounds.maximumMessage);
}

export function boundedArray<T>(
  item: ZodOutput<T>,
  maxItems: number,
  bounds: LengthBounds = {},
): ZodOutput<T[]> {
  return z
    .array(item)
    .min(bounds.minimum ?? 1, bounds.minimumMessage)
    .max(maxItems, bounds.maximumMessage);
}

export function nonEmptyString(
  tool: string,
  field: string,
  maxChars: number,
  example: string,
): ZodOutput<string> {
  return boundedString(maxChars, {
    minimumMessage: `${tool}: ${field} must be a non-empty string. Example: ${example}`,
    maximumMessage: `${tool}: ${field} is too long; expected at most ${maxChars} characters. Example: ${example}`,
  });
}

export function stringArray(
  tool: string,
  field: string,
  maxItems: number,
  maxChars: number,
  example: string,
): ZodOutput<string[]> {
  return boundedArray(nonEmptyString(tool, field, maxChars, example), maxItems, {
    minimumMessage: `${tool}: ${field} must contain at least one non-empty string. Example: ${example}`,
    maximumMessage: `${tool}: ${field} can contain at most ${maxItems} values`,
  });
}

function boundedMap<T>(
  item: ZodOutput<T>,
  maxItems: number,
  maxChars: number,
  label: string,
): ZodOutput<Record<string, T>> {
  return z.preprocess(
    (value, context) => {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
        return value;
      }
      const keys = Object.keys(value);
      if (keys.length > maxItems) {
        context.addIssue({
          code: "custom",
          message: `${label} can contain at most ${maxItems} entries`,
        });
      }
      // Check before z.record normalizes keys, which would overwrite collisions.
      const seen = new Set<string>();
      for (const key of keys) {
        const normalized = key.trim();
        if (seen.has(normalized)) {
          context.addIssue({
            code: "custom",
            message: `${label}: map keys must be distinct after trimming whitespace`,
            path: [key],
          });
        }
        seen.add(normalized);
      }
      return value;
    },
    z.record(boundedString(maxChars), item),
  );
}

function primitiveSchema(maxChars: number): ZodOutput<PrimitiveValue> {
  return z.union([boundedString(maxChars), z.number(), z.boolean()]);
}

export function primitiveMap(
  tool: string,
  field: string,
  options: NormalizedBloombergToolsOptions,
): ZodOutput<PrimitiveMap | undefined> {
  return boundedMap(
    primitiveSchema(options.maxStringChars),
    options.maxFields,
    options.maxStringChars,
    `${tool}: ${field}`,
  ).optional();
}

export function overridesMap(
  tool: string,
  field: string,
  options: NormalizedBloombergToolsOptions,
): ZodOutput<OverrideMap | undefined> {
  const primitive = primitiveSchema(options.maxStringChars);
  const nested = boundedMap(
    primitive,
    options.maxFields,
    options.maxStringChars,
    `${tool}: ${field}`,
  );
  return boundedMap(
    z.union([primitive, nested]),
    options.maxFields + options.maxSecurities,
    options.maxStringChars,
    `${tool}: ${field}`,
  ).optional();
}
