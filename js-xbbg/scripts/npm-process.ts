import { spawnSync } from 'node:child_process';

interface RunOptions {
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly capture?: boolean;
}

function npmCommand(args: readonly string[]): { command: string; args: readonly string[] } {
  const npmExecPath = process.env.npm_execpath;
  if (npmExecPath !== undefined && npmExecPath.length > 0) {
    return { args: [npmExecPath, ...args], command: process.execPath };
  }
  if (process.platform === 'win32') {
    return {
      args: ['/d', '/s', '/c', 'npm.cmd', ...args],
      command: process.env.ComSpec ?? 'cmd.exe',
    };
  }
  return { args, command: 'npm' };
}

export function run(command: string, args: readonly string[], options: RunOptions = {}): string {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    env: options.env,
    encoding: 'utf8',
    stdio: options.capture === true ? 'pipe' : 'inherit',
    windowsHide: true,
  });
  if (result.error !== undefined) {
    throw result.error;
  }
  if (result.status !== 0) {
    if (options.capture === true) {
      process.stderr.write(result.stderr ?? '');
    }
    process.exit(result.status ?? 1);
  }
  return result.stdout ?? '';
}

export function runNpm(args: readonly string[], options: RunOptions = {}): string {
  const invocation = npmCommand(args);
  try {
    return run(invocation.command, invocation.args, options);
  } catch (error) {
    throw new Error(
      `failed to run npm ${args.join(' ')}: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }
}
