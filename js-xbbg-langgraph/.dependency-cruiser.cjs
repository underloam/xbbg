/** @type {import('dependency-cruiser').IConfiguration} */
module.exports = {
  forbidden: [
    {
      name: "no-runtime-cycles",
      comment: "Type-only seams do not create runtime initialization cycles.",
      severity: "error",
      from: { path: "^src/" },
      to: {
        circular: true,
        viaOnly: { dependencyTypesNot: ["type-only"] },
      },
    },
    {
      name: "no-unresolved-imports",
      severity: "error",
      from: { path: "^src/" },
      to: { couldNotResolve: true },
    },
    {
      name: "no-internal-imports-of-public-root",
      comment:
        "The package root composes modules; implementations import their direct dependencies.",
      severity: "error",
      from: { path: "^src/", pathNot: "^src/index\\.ts$" },
      to: { path: "^src/index\\.ts$" },
    },
    {
      name: "no-source-imports-of-test-tooling",
      severity: "error",
      from: { path: "^src/" },
      to: { path: "^(?:test|scripts)/|(?:^|/)[^/]*\\.config\\.[cm]?ts$" },
    },
    {
      name: "no-runtime-dev-dependencies",
      severity: "error",
      from: { path: "^src/" },
      to: {
        dependencyTypes: ["npm-dev"],
        dependencyTypesNot: ["npm", "npm-peer", "npm-optional", "type-only"],
      },
    },
    {
      name: "no-peer-package-implementation-imports",
      comment: "Use package APIs, including the configured @xbbg/core public declaration entry.",
      severity: "error",
      from: { path: "^src/" },
      to: {
        path: "^\\.\\./(?:js-xbbg(?:-langgraph)?|py-xbbg(?:-langgraph)?|bindings|crates)/",
        pathNot: "^\\.\\./js-xbbg/dist/index\\.d\\.ts$",
      },
    },
    {
      name: "support-modules-stay-below-tool-factories",
      severity: "error",
      from: { path: "^src/", pathNot: "^src/(?:index|tools|ext-tools)\\.ts$" },
      to: { path: "^src/(?:tools|ext-tools)\\.ts$" },
    },
    {
      name: "foundations-do-not-import-schema-or-invocation-modules",
      severity: "error",
      from: { path: "^src/(?:_defs_gen|descriptions|options|core-loader|result-[^/]+)\\.ts$" },
      to: { path: "^src/(?:bounded-schemas|schemas|ext-schemas|chart-spec|langchain-tool)\\.ts$" },
    },
    {
      name: "generated-vocabulary-is-a-leaf",
      severity: "error",
      from: { path: "^src/_defs_gen\\.ts$" },
      to: { path: "^src/" },
    },
  ],
  options: {
    // Retain package edges for the rules without traversing dependencies or sibling builds.
    doNotFollow: { path: ["node_modules", "^\\.\\./"] },
    tsConfig: { fileName: "tsconfig.json" },
    tsPreCompilationDeps: true,
    enhancedResolveOptions: {
      exportsFields: ["exports"],
      conditionNames: ["types", "require", "node", "default"],
      mainFields: ["types", "main"],
    },
  },
};
