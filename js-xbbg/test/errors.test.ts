import {
  BlpError,
  BlpInternalError,
  BlpLimitError,
  BlpRequestError,
  BlpSessionError,
  BlpSubscriptionDataLossError,
  BlpTimeoutError,
  BlpValidationError,
  wrapError,
} from '../src/errors';

describe('native error codes', () => {
  it.each([
    ['SESSION', BlpSessionError],
    ['REQUEST', BlpRequestError],
    ['LIMIT', BlpLimitError],
    ['DATALOSS', BlpSubscriptionDataLossError],
    ['VALIDATION', BlpValidationError],
    ['TIMEOUT', BlpTimeoutError],
    ['INTERNAL', BlpInternalError],
    ['UNKNOWN', BlpError],
  ] as const)('maps %s without relying on legacy message patterns', (code, ExpectedError) => {
    const source = new Error(`[XBBG:${code}] synthetic failure`);
    const wrapped = wrapError(source);
    expect(wrapped.constructor).toBe(ExpectedError);
    expect(wrapped.message).toBe('synthetic failure');
    expect(wrapError(source)).toBe(wrapped);
  });
});
