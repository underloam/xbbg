import { runSdkCli } from './sdk';

export function main(argv: readonly string[] = process.argv.slice(2)): number {
  const result = runSdkCli(argv, { prog: 'xbbg-sdk' });
  if (result.stdout.length > 0) {
    process.stdout.write(result.stdout);
  }
  if (result.stderr.length > 0) {
    process.stderr.write(result.stderr);
  }
  return result.exitCode;
}

if (require.main === module) {
  process.exitCode = main();
}
