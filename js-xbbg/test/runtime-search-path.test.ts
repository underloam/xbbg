import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  collectWindowsDapiRootCandidates,
  configureRuntimeSearchPath,
  resolveVendorSdkRoot,
} from '../src/runtime-search-path';

function withTempDir(fn: (root: string) => void): void {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-runtime-search-'));
  try {
    fn(root);
  } finally {
    fs.rmSync(root, { force: true, recursive: true });
  }
}

function winKey(value: string): string {
  return path.win32.normalize(value).toLowerCase();
}

const C_BLP_DAPI = String.raw`C:\blp\DAPI`;
const EXISTING_PATH = String.raw`C:\existing`;
const REPO_ROOT = String.raw`C:\repo`;
const PROGRAM_FILES = String.raw`C:\Program Files`;
const PROGRAM_FILES_X86 = String.raw`C:\Program Files (x86)`;

describe('bloomberg runtime search path discovery', () => {
  it('includes standard Windows Bloomberg DAPI roots', () => {
    const roots = collectWindowsDapiRootCandidates({
      LOCALAPPDATA: String.raw`C:\profile\local`,
      ProgramFiles: PROGRAM_FILES,
      'ProgramFiles(x86)': PROGRAM_FILES_X86,
      SystemDrive: 'C:',
    });

    expect(roots).toContain(C_BLP_DAPI);
    expect(roots).toContain(path.win32.join(PROGRAM_FILES_X86, 'Bloomberg', 'Blp', 'DAPI'));
    expect(roots).toContain(path.win32.join(PROGRAM_FILES, 'Bloomberg', 'Blp', 'DAPI'));
  });

  it('prepends the standard DAPI root when it contains the Bloomberg runtime DLL', () => {
    const dapiRoot = C_BLP_DAPI;
    const runtimeDll = path.win32.join(dapiRoot, 'blpapi3_64.dll');
    const env: NodeJS.ProcessEnv = { PATH: EXISTING_PATH, SystemDrive: 'C:' };

    const selected = configureRuntimeSearchPath({
      env,
      exists: (target) => winKey(target) === winKey(runtimeDll),
      platform: 'win32',
      readDir: () => [],
      repoRoot: REPO_ROOT,
    });

    expect(selected).toBe(dapiRoot);
    expect(env.PATH?.split(';')[0]).toBe(dapiRoot);
  });

  it('detects the Program Files x86 Bloomberg DAPI install root', () => {
    const dapiRoot = path.win32.join(PROGRAM_FILES_X86, 'Bloomberg', 'Blp', 'DAPI');
    const runtimeDll = path.win32.join(dapiRoot, 'blpapi3_64.dll');
    const env: NodeJS.ProcessEnv = { PATH: EXISTING_PATH, SystemDrive: 'C:' };

    const selected = configureRuntimeSearchPath({
      env,
      exists: (target) => winKey(target) === winKey(runtimeDll),
      platform: 'win32',
      readDir: () => [],
      repoRoot: REPO_ROOT,
    });

    expect(selected).toBe(dapiRoot);
    expect(env.PATH?.split(';')[0]).toBe(dapiRoot);
  });

  it('checks bin under BLPAPI_ROOT before standard DAPI fallbacks', () => {
    withTempDir((root) => {
      const sdkRoot = path.join(root, 'sdk');
      const binDir = path.join(sdkRoot, 'bin');
      fs.mkdirSync(binDir, { recursive: true });
      fs.writeFileSync(path.join(binDir, 'blpapi3_64.dll'), 'placeholder');
      const env: NodeJS.ProcessEnv = {
        BLPAPI_ROOT: sdkRoot,
        PATH: EXISTING_PATH,
        SystemDrive: 'C:',
      };

      const selected = configureRuntimeSearchPath({
        env,
        platform: 'win32',
        readDir: () => [],
        repoRoot: root,
      });

      expect(selected).toBe(binDir);
      expect(env.PATH?.split(';')[0]).toBe(binDir);
    });
  });

  it('selects the latest numeric SDK version and skips empty runtime directories', () => {
    withTempDir((root) => {
      const vendorDir = path.join(root, 'vendor', 'blpapi-sdk');
      for (const version of ['3.25.9', '3.25.10', '3.26.0']) {
        fs.mkdirSync(path.join(vendorDir, version, 'bin'), { recursive: true });
      }
      for (const version of ['3.25.9', '3.25.10']) {
        fs.writeFileSync(path.join(vendorDir, version, 'bin', 'blpapi3_64.dll'), 'fixture');
      }

      expect(resolveVendorSdkRoot(root)).toBe(path.join(vendorDir, '3.25.10'));
    });
  });

  it('rejects empty vendor bin and lib directories', () => {
    withTempDir((root) => {
      const vendorDir = path.join(root, 'vendor', 'blpapi-sdk');
      fs.mkdirSync(path.join(vendorDir, '3.25.10', 'bin'), { recursive: true });
      fs.mkdirSync(path.join(vendorDir, '3.25.10', 'lib'));

      expect(resolveVendorSdkRoot(root)).toBeNull();
    });
  });
});
