import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

import type { NativeAddon } from '../src/napi';
import type { TlsConfig } from '../src/types';

const nativePath = path.join(__dirname, '..', 'napi_xbbg.node');
const credentials = Buffer.from([0x30, 0x00, 0xff, 0x01]);
const trust = Uint8Array.from([0x30, 0x00, 0xfe, 0x02]);

// All cases fail extraction or normalization before constructing an SDK session.
describe.skipIf(!existsSync(nativePath))('native TLS configuration boundaries', () => {
  let native: NativeAddon;

  beforeAll(async () => {
    // Load the optional addon only after skipIf excludes unavailable installs.
    // The package also sets Windows' DLL search path.
    await import('../src/index.js');
    native = createRequire(__filename)(nativePath) as NativeAddon;
  });

  test.each<{ name: string; tls: TlsConfig }>([
    { name: 'missing trust', tls: { clientCredentials: credentials } },
    { name: 'missing credentials', tls: { trustMaterial: trust } },
    {
      name: 'blob credentials and path trust',
      tls: { clientCredentials: credentials, trustMaterial: 'fixtures/trust.p7' },
    },
    {
      name: 'path credentials and blob trust',
      tls: { clientCredentials: 'fixtures/client.p12', trustMaterial: trust },
    },
  ])('rejects $name without disclosing material', ({ tls }) => {
    let error: unknown;
    try {
      native.JsEngine.withConfig({
        tls: { ...tls, clientCredentialsPassword: 'synthetic-secret-password' },
      });
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(Error);
    const message = (error as Error).message;
    expect(message).toContain('tls.clientCredentials');
    expect(message).toContain('tls.trustMaterial');
    expect(message).not.toContain('synthetic-secret-password');
    expect(message).not.toContain('fixtures/');
    expect(message).not.toContain(credentials.toString('hex'));
  });

  test.each(['Buffer', 'Uint8Array'] as const)(
    'rejects SharedArrayBuffer-backed %s before async startup',
    async (kind) => {
      const backing = new SharedArrayBuffer(12);
      const view = kind === 'Buffer' ? Buffer.from(backing, 3, 4) : new Uint8Array(backing, 3, 4);
      await expect(async () =>
        native.JsEngine.connectWithConfig({
          tls: {
            clientCredentials: view,
            trustMaterial: trust,
            clientCredentialsPassword: 'synthetic-secret-password',
          },
        }),
      ).rejects.toThrow(/SharedArrayBuffer/u);
    },
  );

  test.each(['Buffer', 'Uint8Array'] as const)(
    'rejects detached %s views before async startup',
    async (kind) => {
      const backing = new ArrayBuffer(12);
      const view = kind === 'Buffer' ? Buffer.from(backing, 3, 4) : new Uint8Array(backing, 3, 4);
      structuredClone(backing, { transfer: [backing] });
      await expect(async () =>
        native.JsEngine.connectWithConfig({
          tls: { clientCredentials: view, trustMaterial: trust },
        }),
      ).rejects.toThrow(/detached/iu);
    },
  );
});
