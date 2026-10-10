/** @type {import('dependency-cruiser').IConfiguration} */
module.exports = {
  forbidden: [
    { name: 'no-circular', severity: 'error', from: {}, to: { circular: true } },
    { name: 'no-orphans', severity: 'error', from: { orphan: true }, to: {} },
    {
      name: 'src-not-to-tooling',
      severity: 'error',
      from: { path: '^src/' },
      to: { path: '^(scripts|test|benchmarks)/' },
    },
    {
      name: 'src-not-to-dev-dependencies',
      severity: 'error',
      from: { path: '^src/' },
      to: { dependencyTypes: ['npm-dev'] },
    },
    { name: 'not-to-unresolvable', severity: 'error', from: {}, to: { couldNotResolve: true } },
  ],
  options: {
    doNotFollow: { path: 'node_modules' },
    // Platform JS/declaration files and dist are generated from the checked TypeScript inputs.
    exclude: String.raw`^dist/|^packages/[^/]+/index\.(js|d\.ts)$`,
    tsPreCompilationDeps: true,
    tsConfig: { fileName: 'tsconfig.json' },
    // The package mixes CommonJS runtime code with ESM-only tooling imports.
    enhancedResolveOptions: {
      exportsFields: ['exports'],
      conditionNames: ['import', 'require', 'node', 'default', 'types'],
    },
  },
};
