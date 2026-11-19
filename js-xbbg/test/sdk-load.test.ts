import path from 'node:path';

import type { LoadedSdk, SdkLocateRequest } from '../src/napi';
import {
  loadBloombergRuntime,
  sdkLoadErrorMessage,
  sdkLocateRequest,
} from '../src/native/sdk-load';

const repoRoot = path.join('synthetic', 'checkout');

const loaded: LoadedSdk = {
  detail: 'BLPAPI_ROOT',
  path: path.join('synthetic', 'libblpapi3_64.so'),
  rejected: [],
  source: 'blpapi_root',
  version: '3.26.5.1',
};

describe(sdkLocateRequest, () => {
  it('hands over the environment it is given and the checkout it was built in', () => {
    const request = sdkLocateRequest({
      env: {
        BLPAPI_LIB_DIR: path.join('synthetic', 'lib'),
        BLPAPI_ROOT: path.join('synthetic', 'root'),
        XBBG_CACHE_DIR: path.join('synthetic', 'cache'),
        XBBG_DEV_SDK_ROOT: 'vendor/sdk',
      },
      repoRoot,
    });

    expect(request).toStrictEqual({
      env: {
        BLPAPI_LIB_DIR: path.join('synthetic', 'lib'),
        BLPAPI_ROOT: path.join('synthetic', 'root'),
        XBBG_CACHE_DIR: path.join('synthetic', 'cache'),
        XBBG_DEV_SDK_ROOT: 'vendor/sdk',
      },
      repoRoot,
    });
  });

  it('passes only the variables the search reads, and treats empty values as unset', () => {
    const request = sdkLocateRequest({
      env: {
        API_TOKEN: 'synthetic-secret',
        BLPAPI_LIB_DIR: '',
        BLPAPI_ROOT: path.join('synthetic', 'root'),
        PATH: ['a', 'b'].join(path.delimiter),
      },
      repoRoot,
    });

    expect(request.env).toStrictEqual({
      BLPAPI_ROOT: path.join('synthetic', 'root'),
      PATH: ['a', 'b'].join(path.delimiter),
    });
  });

  it('sees a value assigned from JavaScript, as Bun and worker threads keep it only in process.env', () => {
    const before = process.env.XBBG_CACHE_DIR;
    process.env.XBBG_CACHE_DIR = path.join('synthetic', 'js-set-cache');
    try {
      expect(sdkLocateRequest({ repoRoot }).env?.XBBG_CACHE_DIR).toBe(
        path.join('synthetic', 'js-set-cache'),
      );
      delete process.env.XBBG_CACHE_DIR;
      expect(sdkLocateRequest({ repoRoot }).env).not.toHaveProperty('XBBG_CACHE_DIR');
    } finally {
      if (before === undefined) {
        delete process.env.XBBG_CACHE_DIR;
      } else {
        process.env.XBBG_CACHE_DIR = before;
      }
    }
  });

  it('reads Windows variables ignoring case and passes them under their canonical names', () => {
    const request = sdkLocateRequest({
      env: {
        LocalAppData: 'synthetic-local',
        Path: 'synthetic-bin',
        'ProgramFiles(x86)': 'synthetic-x86',
        SystemDrive: 'C:',
      },
      platform: 'win32',
      repoRoot,
    });

    expect(request.env).toStrictEqual({
      LOCALAPPDATA: 'synthetic-local',
      PATH: 'synthetic-bin',
      'PROGRAMFILES(X86)': 'synthetic-x86',
      SYSTEMDRIVE: 'C:',
    });
  });

  it('keeps names exact elsewhere', () => {
    const request = sdkLocateRequest({
      env: { Path: 'wrong-case', blpapi_root: 'wrong-case' },
      platform: 'linux',
      repoRoot,
    });

    expect(request.env).toStrictEqual({});
  });
});

describe(loadBloombergRuntime, () => {
  it('loads through the addon with the request it built', () => {
    const requests: (SdkLocateRequest | undefined)[] = [];
    const result = loadBloombergRuntime(
      {
        loadSdk: (request) => {
          requests.push(request);
          return loaded;
        },
      },
      { env: { BLPAPI_ROOT: path.join('synthetic', 'root') }, repoRoot },
    );

    expect(result).toBe(loaded);
    expect(requests).toStrictEqual([
      { env: { BLPAPI_ROOT: path.join('synthetic', 'root') }, repoRoot },
    ]);
  });

  it('throws an error that lists every rejection in order', () => {
    const reason = [
      'no Bloomberg runtime library could be found or loaded:',
      '  blpapi_lib_dir (BLPAPI_LIB_DIR): /synthetic/first/libblpapi3_64.so: not a library',
      '  cache (/synthetic/cache/sdk/3.26.5.1): /synthetic/second/libblpapi3_64.so: checksum mismatch',
    ].join('\n');

    let thrown: unknown;
    try {
      loadBloombergRuntime(
        {
          loadSdk: () => {
            throw new Error(reason);
          },
        },
        { env: {}, repoRoot },
      );
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(Error);
    const message = (thrown as Error).message;
    expect(message).toContain('@xbbg/core could not load the Bloomberg runtime.');
    expect(message).toContain(reason);
    expect(message.indexOf('/synthetic/first/')).toBeLessThan(
      message.indexOf('/synthetic/second/'),
    );
    expect(message).toContain('BLPAPI_LIB_DIR');
    expect((thrown as Error).cause).toBeInstanceOf(Error);
  });
});

describe(sdkLoadErrorMessage, () => {
  it('accepts anything thrown', () => {
    expect(sdkLoadErrorMessage('plain text')).toContain('plain text');
  });
});
