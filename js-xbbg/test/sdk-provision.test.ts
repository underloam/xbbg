import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';

import { sdkLocateRequest } from '../src/native/sdk-load';
import { BLOOMBERG_PYPI_INDEX, installedSdks, provisionSdk, runSdkCli } from '../src/sdk';
import { main as runCliMain } from '../src/sdk-cli';

const nativePath = path.join(__dirname, '..', 'napi_xbbg.node');

/** Build an uncompressed ZIP archive (valid `.whl` with RECORD) in pure Node. */
function writeStoredZip(filePath: string, entries: ReadonlyMap<string, Buffer>): void {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  let offset = 0;

  for (const [name, data] of entries) {
    const nameBuf = Buffer.from(name, 'utf8');
    const crc = zlib.crc32(data);
    const localHeader = Buffer.alloc(30);
    localHeader.writeUInt32LE(0x04034b50, 0);
    localHeader.writeUInt16LE(20, 4);
    localHeader.writeUInt16LE(0, 6);
    localHeader.writeUInt16LE(0, 8); // Store uncompressed
    localHeader.writeUInt16LE(0, 10);
    localHeader.writeUInt16LE(0, 12);
    localHeader.writeUInt32LE(crc, 14);
    localHeader.writeUInt32LE(data.length, 18);
    localHeader.writeUInt32LE(data.length, 22);
    localHeader.writeUInt16LE(nameBuf.length, 26);
    localHeader.writeUInt16LE(0, 28);
    localParts.push(localHeader, nameBuf, data);

    const centralHeader = Buffer.alloc(46);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE(20, 4);
    centralHeader.writeUInt16LE(20, 6);
    centralHeader.writeUInt16LE(0, 8);
    centralHeader.writeUInt16LE(0, 10);
    centralHeader.writeUInt16LE(0, 12);
    centralHeader.writeUInt16LE(0, 14);
    centralHeader.writeUInt32LE(crc, 16);
    centralHeader.writeUInt32LE(data.length, 20);
    centralHeader.writeUInt32LE(data.length, 24);
    centralHeader.writeUInt16LE(nameBuf.length, 28);
    centralHeader.writeUInt16LE(0, 30);
    centralHeader.writeUInt16LE(0, 32);
    centralHeader.writeUInt16LE(0, 34);
    centralHeader.writeUInt16LE(0, 36);
    centralHeader.writeUInt32LE(0, 38);
    centralHeader.writeUInt32LE(offset, 42);
    centralParts.push(centralHeader, nameBuf);

    offset += localHeader.length + nameBuf.length + data.length;
  }

  const centralDir = Buffer.concat(centralParts);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(entries.size, 8);
  eocd.writeUInt16LE(entries.size, 10);
  eocd.writeUInt32LE(centralDir.length, 12);
  eocd.writeUInt32LE(offset, 16);
  eocd.writeUInt16LE(0, 20);

  fs.writeFileSync(filePath, Buffer.concat([...localParts, centralDir, eocd]));
}

function makeSyntheticWheel(dir: string, version = '3.26.9.1'): string {
  const wheelPath = path.join(dir, `blpapi-${version}-cp312-cp312-manylinux_2_17_x86_64.whl`);
  const libMember = 'blpapi/libblpapi3_64.so';
  const libBytes = Buffer.from('synthetic-blpapi-elf-bytes', 'utf8');
  const libDigest = crypto.createHash('sha256').update(libBytes).digest('base64url');
  const distInfo = `blpapi-${version}.dist-info`;
  const metadata = Buffer.from(
    `Metadata-Version: 2.1\nName: blpapi\nVersion: ${version}\n`,
    'utf8',
  );
  const wheelMeta = Buffer.from(
    'Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: false\nTag: cp312-cp312-manylinux_2_17_x86_64\n',
    'utf8',
  );
  const record = Buffer.from(
    `${libMember},sha256=${libDigest},${libBytes.length}\n${distInfo}/RECORD,,\n`,
    'utf8',
  );
  writeStoredZip(
    wheelPath,
    new Map([
      [libMember, libBytes],
      [`${distInfo}/METADATA`, metadata],
      [`${distInfo}/WHEEL`, wheelMeta],
      [`${distInfo}/RECORD`, record],
    ]),
  );
  return wheelPath;
}

describe('sdk provisioning configuration', () => {
  it('sdkLocateRequest forwards XBBG_SDK_SOURCE and XBBG_SDK_VERSION', () => {
    const req = sdkLocateRequest({
      env: {
        XBBG_SDK_SOURCE: 'conda:conda-forge',
        XBBG_SDK_VERSION: '3.26.9.1',
      },
      platform: 'linux',
    });
    expect(req.env?.XBBG_SDK_SOURCE).toBe('conda:conda-forge');
    expect(req.env?.XBBG_SDK_VERSION).toBe('3.26.9.1');
    expect(BLOOMBERG_PYPI_INDEX).toContain('/simple/');
    expect(typeof runCliMain).toBe('function');
  });
});

describe.skipIf(!fs.existsSync(nativePath))('native sdk provisioning (@xbbg/core/sdk)', () => {
  test('provisions a local wheel into the xbbg cache without any Bloomberg runtime loaded', async () => {
    const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'xbbg-js-provision-'));
    try {
      const wheel = makeSyntheticWheel(scratch);
      const cacheDir = path.join(scratch, 'cache');

      const first = await provisionSdk({
        cacheDir,
        platform: 'linux-x86_64',
        source: wheel,
      });
      expect(first.version).toBe('3.26.9.1');
      expect(first.platform).toBe('linux-x86_64');
      expect(first.cacheHit).toBeFalsy();
      expect(fs.existsSync(first.libraryPath)).toBeTruthy();

      const second = await provisionSdk({
        cacheDir,
        platform: 'linux-x86_64',
        source: wheel,
      });
      expect(second.cacheHit).toBeTruthy();
      expect(second.libraryPath).toBe(first.libraryPath);

      const list = installedSdks({ cacheDir, platform: 'linux-x86_64' });
      expect(list).toHaveLength(1);
      expect(list[0]?.version).toBe('3.26.9.1');
      expect(installedSdks({ cacheDir, platform: 'macos-aarch64' })).toHaveLength(0);

      const cliHelp = runSdkCli(['--help'], { env: {} });
      expect(cliHelp.exitCode).toBe(0);
      expect(cliHelp.stdout).toContain('XBBG_SDK_SOURCE');

      const cliInstalled = runSdkCli(['--installed', '--cache-dir', cacheDir], { env: {} });
      expect(cliInstalled.exitCode).toBe(0);
      expect(cliInstalled.stdout).toContain('3.26.9.1');
    } finally {
      fs.rmSync(scratch, { force: true, recursive: true });
    }
  });
});
