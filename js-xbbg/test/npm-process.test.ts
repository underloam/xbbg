import { spawnSync } from 'node:child_process';

import { run, runNpm } from '../scripts/npm-process';

vi.mock(import('node:child_process'));

const originalPlatform = process.platform;
const spawn = vi.mocked(spawnSync);

describe('shared npm process runner', () => {
  beforeEach(() => {
    spawn.mockReset();
    spawn.mockReturnValue({ status: 0, signal: null, pid: 1, output: [], stdout: '', stderr: '' });
    vi.stubEnv('npm_execpath', '');
  });

  afterEach(() => {
    Object.defineProperty(process, 'platform', { value: originalPlatform });
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('prefers npm_execpath and preserves all captured stdout', () => {
    vi.stubEnv('npm_execpath', 'tools/npm-cli.js');
    spawn.mockReturnValue({
      status: 0,
      signal: null,
      pid: 1,
      output: [],
      stdout: 'first line\npackage.tgz\n',
      stderr: '',
    });
    const env = { SYNTHETIC_FLAG: 'enabled' };

    expect(runNpm(['pack', 'package with spaces'], { cwd: 'work', env, capture: true })).toBe(
      'first line\npackage.tgz\n',
    );
    expect(spawn).toHaveBeenCalledWith(
      process.execPath,
      ['tools/npm-cli.js', 'pack', 'package with spaces'],
      { cwd: 'work', env, encoding: 'utf8', stdio: 'pipe', windowsHide: true },
    );
  });

  it.each(['shell.cmd', undefined])('uses Windows ComSpec or cmd.exe: %s', (comspec) => {
    Object.defineProperty(process, 'platform', { value: 'win32' });
    vi.stubEnv('ComSpec', comspec);

    runNpm(['run', 'build:ts']);

    expect(spawn).toHaveBeenCalledWith(
      comspec ?? 'cmd.exe',
      ['/d', '/s', '/c', 'npm.cmd', 'run', 'build:ts'],
      expect.objectContaining({ stdio: 'inherit', windowsHide: true }),
    );
  });

  it('uses npm directly on Unix without npm_execpath', () => {
    Object.defineProperty(process, 'platform', { value: 'linux' });

    runNpm(['pack']);

    expect(spawn).toHaveBeenCalledWith(
      'npm',
      ['pack'],
      expect.objectContaining({ stdio: 'inherit' }),
    );
  });

  it('preserves spawn failure context and cause', () => {
    const error = new Error('spawn unavailable');
    spawn.mockReturnValue({
      status: null,
      signal: null,
      pid: 0,
      output: [],
      stdout: '',
      stderr: '',
      error,
    });

    expect(() => runNpm(['pack'])).toThrow('failed to run npm pack: spawn unavailable');
    expect(() => runNpm(['pack'])).toThrow(expect.objectContaining({ cause: error }));
    expect(() => run('node', [])).toThrow(error);
  });

  it.each([7, null])('propagates failed process status %s and captured stderr', (status) => {
    const stopped = new Error('process exited');
    const exit = vi.spyOn(process, 'exit').mockImplementation(() => {
      throw stopped;
    });
    const stderr = vi.spyOn(process.stderr, 'write').mockReturnValue(true);
    spawn.mockReturnValue({
      status,
      signal: status === null ? 'SIGTERM' : null,
      pid: 1,
      output: [],
      stdout: '',
      stderr: 'npm failed\n',
    });

    expect(() => run('npm', ['pack'], { capture: true })).toThrow(stopped);
    expect(exit).toHaveBeenCalledWith(status ?? 1);
    expect(stderr).toHaveBeenCalledWith('npm failed\n');
  });
});
