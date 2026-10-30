import { BlpError, Engine } from "@xbbg/core";

import { MAX_RESULT_DEPTH } from "../src/_defs_gen";
import { createToolResult } from "../src/result-envelope";
import { projectResult } from "../src/result-limits";
import type { LimitResult, ResultLimitOptions } from "../src/result-types";

// Exercise current public core conversion without loading an addon or opening a session.
vi.mock(import("node:module"), async (importOriginal) => {
  const actual = await importOriginal();
  const nativeAddon = {
    JsEngine: vi.fn<() => never>(() => {
      throw new Error("diagnostic integration tests must not instantiate the native engine");
    }),
    extAuctionFieldGroup: () => ["SYNTHETIC_AUCTION_FIELD"],
    extAuctionZeroPriceFields: () => ["SYNTHETIC_AUCTION_FIELD"],
    extImbalanceSide: () => null,
    getLogLevel: () => "off",
    setLogLevel: () => undefined,
  };

  return {
    ...actual,
    createRequire: (...args: Parameters<typeof actual.createRequire>) => {
      const nodeRequire = actual.createRequire(...args);
      return new Proxy(nodeRequire, {
        apply(target, thisArgument, argumentsList) {
          const moduleId: unknown = argumentsList[0];
          if (typeof moduleId === "string" && /(?:^|[/\\])napi[-_]xbbg\.node$/u.test(moduleId)) {
            return nativeAddon;
          }
          return Reflect.apply(target, thisArgument, argumentsList);
        },
      });
    },
  };
});

vi.mock("../../js-xbbg/src/native/resolve-native.js", () => ({
  resolveNativeAddon: () => ({
    binaryPath: "napi_xbbg.node",
    key: "test",
    packageName: "@xbbg/core-test",
  }),
}));

const EXCEPTIONS = [
  {
    field: "OPEN_INT",
    category: "BAD_FLD",
    code: 9,
    subcategory: "NOT_APPLICABLE",
    message: "synthetic field not applicable",
  },
  {
    field: "BID",
    category: "BAD_FLD",
    code: 10,
    subcategory: "INVALID_FIELD",
    message: "synthetic field refusal",
  },
] as const;

function resultLimits(overrides: Partial<ResultLimitOptions> = {}): ResultLimitOptions {
  return {
    maxResultBytes: 65_536,
    maxResultNodes: 2_000,
    maxRows: 1,
    maxStringChars: 2_000,
    ...overrides,
  };
}

function int32ArrowBatch(values: readonly number[], metadata: Record<string, string> = {}) {
  const data = new Int32Array(values);
  return {
    kind: "zeroCopy",
    columns: [
      {
        name: "answer",
        type: "int32",
        nullable: false,
        length: values.length,
        nullCount: 0,
        data: Buffer.from(data.buffer, data.byteOffset, data.byteLength),
      },
    ],
    metadata,
    numRows: values.length,
  };
}

function emptyFieldInfoArrowBatch(metadata: Record<string, string>) {
  return {
    kind: "zeroCopy",
    columns: ["field", "type", "description", "category", "id"].map((name) => ({
      name,
      type: "utf8",
      nullable: name === "description" || name === "category",
      length: 0,
      nullCount: 0,
      data: Buffer.alloc(0),
      offsets: Buffer.from(new Int32Array([0]).buffer),
    })),
    metadata,
    numRows: 0,
  };
}

function engineWithBatch(batch: unknown) {
  const request = vi.fn(async (_params: unknown) => batch);
  const engine: unknown = Object.create(Engine.prototype);
  if (!(engine instanceof Engine)) {
    throw new TypeError("Expected a public core Engine instance");
  }
  Object.assign(engine, { inner: { request } });
  return { engine, request };
}

function asRows(value: unknown): unknown[] {
  if (!Array.isArray(value)) {
    throw new TypeError("Expected core JSON rows");
  }
  return value;
}

function guardLazyExceptions(rows: unknown[]) {
  const readFieldExceptions = vi.fn<() => never>(() => {
    throw new Error("bounded projection must never read the expanded fieldExceptions getter");
  });
  const guarded = new Proxy(rows, {
    get(target, key, receiver) {
      if (key === "fieldExceptions") {
        return readFieldExceptions();
      }
      return Reflect.get(target, key, receiver);
    },
  });
  return { guarded, readFieldExceptions };
}

function withoutExpandedAccessor(rows: unknown[], metadata?: Record<string, string>): unknown[] {
  const descriptors = Object.fromEntries(
    Object.entries(Object.getOwnPropertyDescriptors(rows)).filter(
      ([key]) => key !== "fieldExceptions",
    ),
  );
  if (metadata !== undefined) {
    descriptors.metadata = { ...descriptors.metadata, value: metadata };
  }
  return Object.defineProperties([], descriptors);
}

function maximumDepth(value: unknown, depth = 0): number {
  if (typeof value !== "object" || value === null) {
    return depth;
  }
  return Object.values(value).reduce<number>(
    (maximum, child: unknown) => Math.max(maximum, maximumDepth(child, depth + 1)),
    depth,
  );
}

function expectProjectionBounds(limited: LimitResult, limits: ResultLimitOptions) {
  const byteLength = new TextEncoder().encode(JSON.stringify(limited.value)).byteLength;
  expect(limited.inspectedNodes).toBeLessThanOrEqual(limits.maxResultNodes);
  expect(limited.truncation?.retainedNodes ?? 0).toBeLessThanOrEqual(limits.maxResultNodes);
  expect(limited.byteLength).toBeLessThanOrEqual(limits.maxResultBytes);
  expect(byteLength).toBe(limited.byteLength);
  expect(maximumDepth(limited.value)).toBeLessThanOrEqual(MAX_RESULT_DEPTH + 1);
}

function expectUndefinedOptionalMetadata(rows: unknown[], projected: object) {
  for (const key of [
    "bloombergRequestId",
    "bloombergRequestIds",
    "delayed",
    "eidData",
    "securityErrors",
  ]) {
    const descriptor = Object.getOwnPropertyDescriptor(rows, key);
    expect(descriptor).toMatchObject({ enumerable: true, value: undefined });
    expect(Object.hasOwn(descriptor ?? {}, "value")).toBe(true);
    expect(Object.hasOwn(projected, key)).toBe(false);
  }
}

describe("current core diagnostic conversion into bounded LangGraph results", () => {
  it("retains an ordered expanded prefix when the raw 12k dictionary fits inspection budgets", async () => {
    const securities = Object.fromEntries([
      ["__proto__", [1, 0, 1]],
      ["constructor", [0, 1, 0]],
      ["SYN", Array.from({ length: 12_001 }, (_, index) => (index % 3 === 2 ? 1 : 0))],
    ]);
    const raw = JSON.stringify({ version: 1, entries: EXCEPTIONS, securities });
    const metadata = Object.freeze({ "xbbg.field_exceptions": raw });
    const batch = int32ArrowBatch([7, 8], metadata);
    const { engine, request } = engineWithBatch(batch);
    const rows = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
    const descriptor = Object.getOwnPropertyDescriptor(rows, "fieldExceptions");
    expect(descriptor).toHaveProperty("get", expect.any(Function));
    expect(descriptor?.enumerable).toBe(true);
    expect(Object.hasOwn(descriptor ?? {}, "value")).toBe(false);
    const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
    const limits = resultLimits({
      maxResultBytes: 65_536,
      maxResultNodes: 100_000,
      maxStringChars: 128,
    });

    const limited = projectResult(guarded, limits, false);
    const projected = limited.value as Record<string, any>;
    const expanded = projected.fieldExceptions as Record<string, unknown[]>;

    expect(readFieldExceptions).not.toHaveBeenCalled();
    expect(request).toHaveBeenCalledOnce();
    expect(request.mock.calls[0]?.[0]).toMatchObject({
      operation: "ReferenceDataRequest",
      securities: ["SYN"],
    });
    expect(Object.getOwnPropertyDescriptor(expanded, "__proto__")?.value).toEqual([
      EXCEPTIONS[1],
      EXCEPTIONS[0],
      EXCEPTIONS[1],
    ]);
    expect(Object.getOwnPropertyDescriptor(expanded, "constructor")?.value).toEqual([
      EXCEPTIONS[0],
      EXCEPTIONS[1],
      EXCEPTIONS[0],
    ]);
    const retained = expanded.SYN ?? [];
    expect(retained.length).toBeGreaterThan(2);
    expect(retained.length).toBeLessThan(12_001);
    for (const [security, occurrences] of Object.entries(expanded)) {
      const indices = Object.getOwnPropertyDescriptor(securities, security)?.value as number[];
      expect(occurrences).toEqual(
        indices.slice(0, occurrences.length).map((index) => EXCEPTIONS[index]),
      );
      expect(
        occurrences.every((occurrence) => typeof occurrence === "object" && occurrence !== null),
      ).toBe(true);
    }
    expect(Object.hasOwn(expanded, "entries")).toBe(false);
    expect(Object.hasOwn(expanded, "securities")).toBe(false);
    const serialized = JSON.parse(JSON.stringify(expanded)) as object;
    expect(Object.hasOwn(serialized, "__proto__")).toBe(true);
    expect(limited.hasErrors).toBe(true);
    expect(limited.truncated).toBe(true);
    expect(limited.truncation?.reasons).toContain("max_result_bytes");
    expect(limited.rowCount).toBe(2);
    expect(limited.retainedRows).toBeLessThanOrEqual(1);
    expectProjectionBounds(limited, limits);
    expect(batch.metadata).toEqual({ "xbbg.field_exceptions": raw });
    expect(Object.getOwnPropertyDescriptor(rows, "metadata")?.value).toEqual(metadata);
  });

  it.each([false, true])(
    "keeps fitting diagnostic order and multiplicity independent of data maxRows (raw-only=%s)",
    async (rawOnly) => {
      const securities = Object.fromEntries([
        ["__proto__", [1, 0, 1]],
        ["constructor", [0, 1]],
      ]);
      const raw = JSON.stringify({ version: 1, entries: EXCEPTIONS, securities });
      const metadata = Object.freeze({ "xbbg.field_exceptions": raw });
      const { engine } = engineWithBatch(int32ArrowBatch([7, 8, 9], metadata));
      const converted = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
      const rows = rawOnly ? withoutExpandedAccessor(converted) : converted;
      expect(Object.hasOwn(rows, "fieldExceptions")).toBe(!rawOnly);
      const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
      const limits = resultLimits();

      const limited = projectResult(guarded, limits, false);
      const projected = limited.value as Record<string, any>;

      expect(readFieldExceptions).not.toHaveBeenCalled();
      expect(projected.rows).toEqual([{ answer: 7 }]);
      expect(
        Object.getOwnPropertyDescriptor(projected.fieldExceptions, "__proto__")?.value,
      ).toEqual([EXCEPTIONS[1], EXCEPTIONS[0], EXCEPTIONS[1]]);
      expect(
        Object.getOwnPropertyDescriptor(projected.fieldExceptions, "constructor")?.value,
      ).toEqual([EXCEPTIONS[0], EXCEPTIONS[1]]);
      expect(limited.rowCount).toBe(3);
      expect(limited.retainedRows).toBe(1);
      expect(limited.truncation?.reasons).toEqual(["max_rows"]);
      expect(limited.truncation?.omittedRows).toBe(2);
      expect(projected.metadata["xbbg.field_exceptions"]).toBe(raw);
      expectUndefinedOptionalMetadata(rows, projected);
      expectProjectionBounds(limited, limits);
      expect(Object.getOwnPropertyDescriptor(converted, "metadata")?.value).toEqual(metadata);
    },
  );

  it("omits actual enumerable undefined core metadata without false truncation", async () => {
    const { engine } = engineWithBatch(int32ArrowBatch([7]));
    const rows = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
    const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
    const limits = resultLimits();

    const limited = projectResult(guarded, limits, false);
    const projected = limited.value as Record<string, unknown>;
    const [content, artifact] = createToolResult("xbbg_bdp", guarded, {
      ...limits,
      maxContentBytes: 8_192,
      maxContentRows: 1,
    });

    expect(readFieldExceptions).not.toHaveBeenCalled();
    expectUndefinedOptionalMetadata(rows, projected);
    expect(Object.getOwnPropertyDescriptor(rows, "fieldErrors")).toMatchObject({
      enumerable: true,
      value: undefined,
    });
    expect(Object.hasOwn(projected, "fieldErrors")).toBe(false);
    expect(Object.hasOwn(projected, "fieldExceptions")).toBe(false);
    expect(limited).toMatchObject({
      hasErrors: false,
      rowCount: 1,
      truncated: false,
      value: { metadata: {}, rows: [{ answer: 7 }] },
    });
    expect(limited.truncation).toBeUndefined();
    expect(artifact).toMatchObject({ rowCount: 1, truncated: false });
    expect(artifact.truncation).toBeUndefined();
    expect(content).not.toContain("unsupported_value");
    expect(content).not.toContain("invalid_entitlement_data");
    expectProjectionBounds(limited, limits);
  });

  it("retains all-rejected current FieldInfo reasons and source without false truncation", async () => {
    const error = {
      category: "BAD_FLD",
      code: -103,
      subcategory: "INVALID_FIELD",
      message: "synthetic Unknown Field Id/Mnemonic",
      source: "synthetic-apiflds",
    };
    const fieldErrors = Object.fromEntries([
      ["__proto__", error],
      ["constructor", { ...error, code: -104 }],
    ]);
    const raw = JSON.stringify(fieldErrors);
    const metadata = Object.freeze({ "xbbg.field_errors": raw });
    const { engine, request } = engineWithBatch(emptyFieldInfoArrowBatch(metadata));
    const rows = asRows(
      await engine.bflds({ fields: ["__proto__", "constructor"], backend: "json" }),
    );
    const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
    const limits = resultLimits();

    const limited = projectResult(guarded, limits, false);
    const projected = limited.value as Record<string, any>;
    const [content, artifact] = createToolResult("xbbg_bflds", guarded, {
      ...limits,
      maxContentBytes: 8_192,
      maxContentRows: 1,
    });

    expect(readFieldExceptions).not.toHaveBeenCalled();
    expect(request.mock.calls[0]?.[0]).toMatchObject({ operation: "FieldInfoRequest" });
    expectUndefinedOptionalMetadata(rows, projected);
    expect(projected.fieldErrors).toEqual(fieldErrors);
    expect(Object.getOwnPropertyDescriptor(projected.fieldErrors, "__proto__")?.value).toEqual(
      error,
    );
    expect(Object.hasOwn(projected, "fieldExceptions")).toBe(false);
    expect(limited.rowCount).toBe(0);
    expect(limited.truncated).toBe(false);
    expect(limited.truncation).toBeUndefined();
    expect(artifact).toMatchObject({
      data: { fieldErrors, rows: [] },
      hasErrors: true,
      rowCount: 0,
      truncated: false,
    });
    expect(artifact.truncation).toBeUndefined();
    expect(content).toContain(error.message);
    expect(content).toContain(error.source);
    expect(content).not.toContain("unsupported_value");
    expect(content).not.toContain("invalid_entitlement_data");
    expectProjectionBounds(limited, limits);
    expect(Object.getOwnPropertyDescriptor(rows, "metadata")?.value).toEqual(metadata);
    expect(metadata["xbbg.field_errors"]).toBe(raw);
  });

  it("applies the shared string budget to lazily converted diagnostic payloads", async () => {
    const exception = { ...EXCEPTIONS[0], message: "synthetic detail ".repeat(1_000) };
    const raw = JSON.stringify({
      version: 1,
      entries: [exception],
      securities: { SYN: [0, 0, 0] },
    });
    const metadata = Object.freeze({ "xbbg.field_exceptions": raw });
    const { engine } = engineWithBatch(int32ArrowBatch([7], metadata));
    const rows = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
    const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
    const limits = resultLimits({
      maxResultBytes: 65_536,
      maxResultNodes: 100_000,
      maxStringChars: 32,
    });

    const limited = projectResult(guarded, limits, false);
    const projected = limited.value as Record<string, any>;

    expect(readFieldExceptions).not.toHaveBeenCalled();
    expect(projected.fieldExceptions.SYN).toHaveLength(3);
    for (const occurrence of projected.fieldExceptions.SYN as Record<string, unknown>[]) {
      expect(occurrence.message).toBe(
        `${exception.message.slice(0, limits.maxStringChars)}…[truncated ${String(exception.message.length - limits.maxStringChars)} chars]`,
      );
    }
    expect(projected.rows).toEqual([{ answer: 7 }]);
    expect(limited.truncation?.reasons).toEqual(["max_string_chars"]);
    expectProjectionBounds(limited, limits);
    expect(Object.getOwnPropertyDescriptor(rows, "metadata")?.value).toEqual(metadata);
    expect(metadata["xbbg.field_exceptions"]).toBe(raw);
  });

  it.each([MAX_RESULT_DEPTH - 3, MAX_RESULT_DEPTH - 2])(
    "does not replace diagnostic members at the nested depth boundary (%s wrappers)",
    async (wrappers) => {
      const raw = JSON.stringify({
        version: 1,
        entries: EXCEPTIONS,
        securities: { SYN: [0, 0, 0] },
      });
      const metadata = Object.freeze({ "xbbg.field_exceptions": raw });
      const { engine } = engineWithBatch(int32ArrowBatch([7], metadata));
      const rows = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
      const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
      let nested: unknown = guarded;
      for (let index = 0; index < wrappers; index += 1) {
        nested = { child: nested };
      }
      const limits = resultLimits({
        maxResultBytes: 4_096,
        maxResultNodes: 2_000,
        maxStringChars: 128,
      });

      const limited = projectResult(nested, limits, false);
      let projected = limited.value as Record<string, any>;
      for (let index = 0; index < wrappers; index += 1) {
        projected = projected.child as Record<string, any>;
      }

      expect(readFieldExceptions).not.toHaveBeenCalled();
      expect(limited.truncation?.reasons).toContain("max_result_depth");
      expect(Object.values(projected.fieldExceptions ?? {}).flat()).toEqual([]);
      expectProjectionBounds(limited, limits);
      expect(Object.getOwnPropertyDescriptor(rows, "metadata")?.value).toEqual(metadata);
    },
  );

  it.each([
    { raw: "{", reason: /valid JSON/iu },
    {
      raw: JSON.stringify({ entries: [], securities: {} }),
      reason: /compact envelope/iu,
    },
    {
      raw: JSON.stringify({ version: 1, entries: [], securities: { SYN: [0] } }),
      reason: /dictionary index/iu,
    },
    {
      raw: JSON.stringify({
        version: 1,
        entries: [{ ...EXCEPTIONS[0], field: 42 }],
        securities: { SYN: [0] },
      }),
      reason: /exception entry/iu,
    },
  ])("retains malformed raw-only fallback context: $reason", async ({ raw, reason }) => {
    const { engine } = engineWithBatch(int32ArrowBatch([7]));
    const converted = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
    // Older cores may expose persisted raw metadata without a convenience accessor.
    const metadata = Object.freeze({ "xbbg.field_exceptions": raw });
    const rows = withoutExpandedAccessor(converted, metadata);
    const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
    let error: unknown;

    try {
      projectResult(guarded, resultLimits(), false);
    } catch (cause) {
      error = cause;
    }

    expect(error).toBeInstanceOf(TypeError);
    expect(error).toHaveProperty("message", expect.stringMatching(/xbbg\.field_exceptions/iu));
    expect(error).toHaveProperty("message", expect.stringMatching(reason));
    expect(readFieldExceptions).not.toHaveBeenCalled();
    expect(Object.getOwnPropertyDescriptor(rows, "metadata")?.value).toEqual(metadata);
    expect(Object.getOwnPropertyDescriptor(converted, "metadata")?.value).toEqual({});
  });

  it.each([
    { name: "invalid JSON", raw: "{", reason: /valid JSON/iu },
    {
      name: "unversioned reserved envelope",
      raw: JSON.stringify({ entries: [], securities: {} }),
      reason: /version/iu,
    },
    {
      name: "invalid dictionary index after 12k valid occurrences",
      raw: JSON.stringify({
        version: 1,
        entries: EXCEPTIONS,
        securities: { SYN: [...Array.from({ length: 12_001 }, () => 0), 2] },
      }),
      reason: /index/iu,
    },
  ])(
    "keeps malformed raw metadata context before bounded projection: $name",
    async ({ raw, reason }) => {
      const metadata = Object.freeze({ "xbbg.field_exceptions": raw });
      const { engine, request } = engineWithBatch(int32ArrowBatch([7], metadata));

      const converted = engine.request({
        backend: "json",
        service: "//blp/refdata",
        operation: "ReferenceDataRequest",
        extractor: "refdata",
        securities: ["SYN"],
        fields: ["BID"],
      });

      await expect(converted).rejects.toBeInstanceOf(BlpError);
      await expect(converted).rejects.toThrow(/xbbg\.field_exceptions/iu);
      await expect(converted).rejects.toThrow(reason);
      expect(request).toHaveBeenCalledOnce();
      expect(metadata["xbbg.field_exceptions"]).toBe(raw);
    },
  );

  it.each([
    { maxResultBytes: 4_096, maxResultNodes: 10_000, reason: "max_result_bytes" },
    { maxResultBytes: 65_536, maxResultNodes: 128, reason: "max_result_nodes" },
  ])(
    "omits oversized raw diagnostics before parsing without expanding the core getter: $reason",
    async (budget) => {
      const raw = JSON.stringify({
        version: 1,
        entries: EXCEPTIONS,
        securities: { SYN: Array.from({ length: 12_001 }, () => 0) },
      });
      const { engine } = engineWithBatch(int32ArrowBatch([7], { "xbbg.field_exceptions": raw }));
      const rows = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
      const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
      const limits = resultLimits(budget);
      const limited = projectResult(guarded, limits, false);
      const projected = limited.value as Record<string, unknown>;

      expect(readFieldExceptions).not.toHaveBeenCalled();
      expect(projected.fieldExceptions).toBeUndefined();
      expect(projected.rows).toEqual([{ answer: 7 }]);
      expect(limited.hasErrors).toBe(true);
      expect(limited.truncation?.reasons).toContain(budget.reason);
      expectProjectionBounds(limited, limits);
    },
  );

  it("does not parse oversized invalid raw-only metadata", async () => {
    const { engine } = engineWithBatch(int32ArrowBatch([7]));
    const converted = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
    const rows = withoutExpandedAccessor(converted, {
      "xbbg.field_exceptions": "{".repeat(20_000),
    });
    const limits = resultLimits({ maxResultBytes: 4_096, maxResultNodes: 256 });
    const limited = projectResult(rows, limits, false);

    expect(limited.hasErrors).toBe(true);
    expect(limited.truncation?.reasons).toContain("max_result_nodes");
    expect((limited.value as Record<string, unknown>).rows).toEqual([{ answer: 7 }]);
    expectProjectionBounds(limited, limits);
  });

  it.each([
    { entries: [EXCEPTIONS[0], null], indices: [0] },
    { entries: EXCEPTIONS, indices: [...Array.from({ length: 300 }, () => 0), 2] },
  ])(
    "validates every admitted entry and reference even after another error was reported",
    async ({ entries, indices }) => {
      const { engine } = engineWithBatch(
        int32ArrowBatch([7], {
          "xbbg.security_errors": JSON.stringify({
            SYN_BAD: { message: "synthetic adjacent failure" },
          }),
        }),
      );
      const converted = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
      const rows = withoutExpandedAccessor(converted, {
        "xbbg.field_exceptions": JSON.stringify({
          version: 1,
          entries,
          securities: { SYN: indices },
        }),
      });

      expect(() =>
        projectResult(rows, resultLimits({ maxResultBytes: 2_048, maxResultNodes: 10_000 }), false),
      ).toThrow(/xbbg\.field_exceptions.*(?:entry|index)/iu);
    },
  );

  it.each([
    { entries: [], securities: {} },
    { entries: [], securities: { SYN_EMPTY: [] } },
    { entries: EXCEPTIONS, securities: { SYN_EMPTY: [] } },
  ])("does not report errors for compact dictionaries without occurrences: %o", async (payload) => {
    const raw = JSON.stringify({ version: 1, ...payload });
    const { engine } = engineWithBatch(int32ArrowBatch([7], { "xbbg.field_exceptions": raw }));
    const rows = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
    const limited = projectResult(rows, resultLimits(), false);

    expect(limited.hasErrors).toBe(false);
    expect((limited.value as Record<string, unknown>).fieldExceptions).toEqual(payload.securities);
    expect(limited.truncated).toBe(false);
  });

  it("retains refused ids named eidData and eidDataTruncation without entitlement interpretation", async () => {
    const refusal = {
      category: "BAD_FLD",
      code: -103,
      subcategory: "INVALID",
      message: "synthetic unknown field",
      source: "synthetic-apiflds",
    };
    const fieldErrors = { eidData: refusal, eidDataTruncation: refusal };
    const { engine } = engineWithBatch(
      emptyFieldInfoArrowBatch({
        "xbbg.field_errors": JSON.stringify(fieldErrors),
      }),
    );
    const rows = asRows(
      await engine.bflds({ fields: ["eidData", "eidDataTruncation"], backend: "json" }),
    );
    const limited = projectResult(rows, resultLimits(), false);

    expect((limited.value as Record<string, unknown>).fieldErrors).toEqual(fieldErrors);
    expect(limited.hasErrors).toBe(true);
    expect(limited.truncated).toBe(false);
    expect(limited.truncation).toBeUndefined();
  });

  it("never retains partial exception objects when re-projecting into a smaller artifact", async () => {
    const exception = {
      ...EXCEPTIONS[0],
      field: "SYN_FIELD_".repeat(32),
      category: "SYN_CATEGORY_".repeat(24),
      subcategory: "SYN_SUBCATEGORY_".repeat(20),
      message: "synthetic diagnostic detail ".repeat(40),
    };
    const raw = JSON.stringify({
      version: 1,
      entries: [exception],
      securities: { SYN: [0, 0, 0] },
    });
    const { engine } = engineWithBatch(int32ArrowBatch([7], { "xbbg.field_exceptions": raw }));
    const rows = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
    const [content, artifact] = createToolResult("xbbg_bdp", rows, {
      ...resultLimits({ maxResultBytes: 2_048, maxResultNodes: 100_000, maxStringChars: 2_000 }),
      maxContentBytes: 65_536,
      maxContentRows: 1,
    });
    const data = artifact.data as Record<string, any>;
    const retained = (data?.fieldExceptions?.SYN ?? []) as Record<string, unknown>[];

    expect(content).toContain(exception.message);
    expect(artifact.hasErrors).toBe(true);
    expect(artifact.truncated).toBe(true);
    for (const occurrence of retained) {
      expect(Object.keys(occurrence).sort()).toEqual(Object.keys(exception).sort());
      expect(occurrence.code).toBe(exception.code);
      expect(occurrence.message).toBeTypeOf("string");
    }
    expect(new TextEncoder().encode(JSON.stringify(artifact)).byteLength).toBeLessThanOrEqual(
      2_048,
    );
  });

  it("preserves possible-error presence when the raw dictionary cannot receive a node visit", async () => {
    const raw = JSON.stringify({ version: 1, entries: EXCEPTIONS, securities: { SYN: [0] } });
    const { engine } = engineWithBatch(int32ArrowBatch([7], { "xbbg.field_exceptions": raw }));
    const rows = asRows(await engine.bdp(["SYN"], ["BID"], { backend: "json" }));
    const { guarded, readFieldExceptions } = guardLazyExceptions(rows);
    const limited = projectResult(guarded, resultLimits({ maxResultNodes: 1 }), false);

    expect(readFieldExceptions).not.toHaveBeenCalled();
    expect(limited.hasErrors).toBe(true);
    expect(limited.truncation?.reasons).toContain("max_result_nodes");
    expect(limited.inspectedNodes).toBeLessThanOrEqual(1);
  });

  it("does not invoke expanded diagnostic getters while determining error presence", () => {
    const getter = vi.fn(() => {
      throw new Error("synthetic getter must not execute");
    });
    const exceptions = Object.defineProperty({}, "SYN", { enumerable: true, get: getter });
    const limited = projectResult({ fieldExceptions: exceptions }, resultLimits(), false);

    expect(getter).not.toHaveBeenCalled();
    expect(limited.hasErrors).toBe(true);
    expect(limited.truncation?.reasons).toContain("accessor_omitted");
  });
});
