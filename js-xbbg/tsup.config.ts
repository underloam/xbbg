import { defineConfig } from 'tsup';

const shared = {
  format: ['cjs'] as ['cjs'],
  outDir: 'dist',
  platform: 'node' as const,
  shims: false,
  sourcemap: true,
  splitting: false,
  target: 'node24' as const,
  treeshake: true,
};

export default defineConfig([
  {
    ...shared,
    clean: true,
    dts: true,
    entry: ['src/index.ts', 'src/sdk.ts'],
  },
  {
    ...shared,
    banner: { js: '#!/usr/bin/env node' },
    clean: false,
    dts: false,
    entry: ['src/sdk-cli.ts'],
  },
]);
