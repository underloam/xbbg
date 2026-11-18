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

describe('sdk request provenance', () => {
  it.each(['REQUEST', 'LIMIT', 'TIMEOUT'])(
    'preserves structured %s context without conflating labels',
    (code) => {
      const ids = ['sdk-synthetic-]"\\\n', 'sdk-synthetic-z'];
      for (const count of [1, 2]) {
        const context = {
          bloomberg_request_id: count === 1 ? ids[0] : null,
          bloomberg_request_ids: ids.slice(0, count),
          operation: 'SyntheticRequest',
          request_id: 'req-label',
          service: '//synthetic/refdata',
        };
        const message =
          code === 'TIMEOUT'
            ? 'Request timed out [request_id=req-label]'
            : 'Request failed on //synthetic/refdata::SyntheticRequest [request_id=req-label]';
        const source = new Error(
          `[XBBG:${code}] [XBBG_CONTEXT]${JSON.stringify(context)}\n${message}`,
        );
        const wrapped = wrapError(source);
        expect(wrapped).toBeInstanceOf(code === 'TIMEOUT' ? BlpTimeoutError : BlpRequestError);
        if (!(wrapped instanceof BlpRequestError || wrapped instanceof BlpTimeoutError)) {
          throw new Error('expected structured request error');
        }
        expect(wrapped.message).toBe(message);
        expect(wrapped.request_id).toBe('req-label');
        expect(wrapped.bloomberg_request_id).toBe(count === 1 ? ids[0] : undefined);
        expect(wrapped.bloomberg_request_ids).toStrictEqual(ids.slice(0, count));
        expect(wrapError(source)).toBe(wrapped);
      }
    },
  );

  it.each([
    ['SESSION', BlpSessionError],
    ['VALIDATION', BlpValidationError],
    ['DATALOSS', BlpSubscriptionDataLossError],
    ['INTERNAL', BlpInternalError],
  ] as const)(
    'retains completed contributor IDs without reclassifying %s',
    (code, ExpectedError) => {
      const ids = ['sdk-synthetic-a', 'sdk-synthetic-z'];
      const context = { bloomberg_request_id: null, bloomberg_request_ids: ids };
      const source = new Error(
        `[XBBG:${code}] [XBBG_CONTEXT]${JSON.stringify(context)}\nsynthetic recipe failure`,
      );
      const wrapped = wrapError(source);
      expect(wrapped.constructor).toBe(ExpectedError);
      expect(wrapped.message).toBe('synthetic recipe failure');
      expect(wrapped.bloomberg_request_id).toBeUndefined();
      expect(wrapped.bloomberg_request_ids).toStrictEqual(ids);
    },
  );

  it('does not fabricate SDK identifiers from an xbbg label', () => {
    const error = wrapError(
      new Error(
        '[XBBG:REQUEST] Request failed on //synthetic/refdata::SyntheticRequest [request_id=req-label]',
      ),
    );
    expect(error).toBeInstanceOf(BlpRequestError);
    if (!(error instanceof BlpRequestError)) {
      throw new Error('expected request error');
    }
    expect(error.request_id).toBe('req-label');
    expect(error.bloomberg_request_id).toBeUndefined();
    expect(error.bloomberg_request_ids).toBeUndefined();
  });
});

describe('host validation context', () => {
  it('keeps the offending element and suggestion from the context line', () => {
    const context = { element: 'requestTimeoutMs', suggestion: 'requestTimeoutMs' };
    const wrapped = wrapError(
      new Error(`[XBBG:VALIDATION] [XBBG_CONTEXT]${JSON.stringify(context)}\nsynthetic failure`),
    );
    expect(wrapped).toBeInstanceOf(BlpValidationError);
    if (!(wrapped instanceof BlpValidationError)) {
      throw new Error('expected validation error');
    }
    expect(wrapped.message).toBe('synthetic failure');
    expect(wrapped.element).toBe('requestTimeoutMs');
    expect(wrapped.suggestion).toBe('requestTimeoutMs');
  });

  it('does not let an absent suggestion hide one parsed from the message', () => {
    const wrapped = wrapError(
      new Error(
        `[XBBG:VALIDATION] [XBBG_CONTEXT]${JSON.stringify({ element: 'fields' })}\nunknown (did you mean 'PX_LAST'?)`,
      ),
    );
    if (!(wrapped instanceof BlpValidationError)) {
      throw new Error('expected validation error');
    }
    expect(wrapped.element).toBe('fields');
    expect(wrapped.suggestion).toBe('PX_LAST');
  });

  it('treats a malformed native argument as a validation error', () => {
    const wrapped = wrapError(new Error('[XBBG:ARGUMENT] unknown field synthetic'));
    expect(wrapped).toBeInstanceOf(BlpValidationError);
    expect(wrapped.message).toBe('unknown field synthetic');
  });
});
