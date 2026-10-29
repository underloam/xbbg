import { isPlainObject } from './objects';

export interface BlpRequestErrorOptions {
  readonly service?: string;
  readonly operation?: string;
  readonly request_id?: string;
  readonly bloomberg_request_id?: string;
  readonly bloomberg_request_ids?: readonly string[];
  readonly code?: string | number;
}

export interface BlpValidationErrorOptions extends Pick<
  BlpRequestErrorOptions,
  'bloomberg_request_id' | 'bloomberg_request_ids'
> {
  readonly element?: string;
  readonly suggestion?: string;
}

export interface BlpSubscriptionDataLossErrorOptions extends Pick<
  BlpRequestErrorOptions,
  'bloomberg_request_id' | 'bloomberg_request_ids'
> {
  readonly topic?: string;
  readonly detail?: string;
}

export class BlpError extends Error {
  public readonly bloomberg_request_id?: string;
  public readonly bloomberg_request_ids?: readonly string[];

  public constructor(message: string, options: BlpRequestErrorOptions = {}) {
    super(message);
    this.name = new.target.name;
    this.bloomberg_request_id = options.bloomberg_request_id;
    this.bloomberg_request_ids = options.bloomberg_request_ids;
  }
}

export class BlpSessionError extends BlpError {}

export class BlpRequestError extends BlpError {
  public readonly service?: string;
  public readonly operation?: string;
  public readonly request_id?: string;
  public readonly code?: string | number;

  public constructor(message: string, options: BlpRequestErrorOptions = {}) {
    super(message, options);
    this.service = options.service;
    this.operation = options.operation;
    this.request_id = options.request_id;
    this.code = options.code;
  }
}

export class BlpLimitError extends BlpRequestError {}

export class BlpSubscriptionDataLossError extends BlpError {
  public readonly code = 'DATALOSS' as const;
  public readonly topic?: string;
  public readonly detail?: string;

  public constructor(message: string, options: BlpSubscriptionDataLossErrorOptions = {}) {
    super(message, options);
    this.topic = options.topic;
    this.detail = options.detail;
  }
}

export class BlpValidationError extends BlpError {
  public readonly element?: string;
  public readonly suggestion?: string;

  public constructor(message: string, options: BlpValidationErrorOptions = {}) {
    super(message, options);
    this.element = options.element;
    this.suggestion = options.suggestion;
  }
}

export class BlpTimeoutError extends BlpError {
  public readonly request_id?: string;

  public constructor(message: string, options: BlpRequestErrorOptions = {}) {
    super(message, options);
    this.request_id = options.request_id;
  }
}

export class BlpInternalError extends BlpError {}

/**
 * Machine-readable code prefix emitted by the native layer:
 * `[XBBG:<CODE>] <message>`. Parsed first; the legacy message-pattern
 * heuristics below remain as a fallback for non-coded errors.
 */
const CODE_PREFIX = /^\[XBBG:([A-Z]+)\]\s*/u;

const wrappedNativeErrors = new WeakMap<Error, BlpError>();

function cacheWrappedError(source: unknown, wrapped: BlpError): BlpError {
  if (source instanceof Error) {
    wrappedNativeErrors.set(source, wrapped);
  }
  return wrapped;
}

function fromCode(code: string, msg: string, context: BlpRequestErrorOptions = {}): BlpError {
  const requestOptions = { ...parseRequestOptions(msg), ...context };
  switch (code) {
    case 'SESSION':
      return new BlpSessionError(msg, requestOptions);
    case 'REQUEST':
      return new BlpRequestError(msg, requestOptions);
    case 'LIMIT':
      return new BlpLimitError(msg, requestOptions);
    case 'DATALOSS':
      return new BlpSubscriptionDataLossError(msg, {
        ...parseSubscriptionDataLossOptions(msg),
        ...context,
      });
    case 'VALIDATION':
      return new BlpValidationError(msg, { ...parseValidationOptions(msg), ...context });
    case 'TIMEOUT':
      return new BlpTimeoutError(msg, requestOptions);
    case 'INTERNAL':
      return new BlpInternalError(msg, requestOptions);
    default:
      return new BlpError(msg, requestOptions);
  }
}

export function wrapError(napiError: unknown): BlpError {
  if (napiError instanceof BlpError) {
    return napiError;
  }
  if (napiError instanceof Error) {
    const cached = wrappedNativeErrors.get(napiError);
    if (cached !== undefined) {
      return cached;
    }
  }
  const msg =
    napiError instanceof Error ? napiError.message : typeof napiError === 'string' ? napiError : '';

  const codeMatch = CODE_PREFIX.exec(msg);
  const nativeCode = codeMatch?.[1];
  const matchedPrefix = codeMatch?.[0];
  if (nativeCode !== undefined && matchedPrefix !== undefined) {
    const decoded = decodeRequestContext(msg.slice(matchedPrefix.length));
    return cacheWrappedError(napiError, fromCode(nativeCode, decoded.message, decoded.options));
  }

  // Session errors
  if (
    msg.includes('Session start failed') ||
    msg.includes('session start failed') ||
    msg.includes('Failed to start session') ||
    msg.includes('Failed to open service') ||
    msg.includes('failed to spawn worker') ||
    msg.includes('connect event failed')
  ) {
    return cacheWrappedError(napiError, new BlpSessionError(msg));
  }

  // Request errors
  if (msg.includes('Request failed') || msg.includes('Subscription failed')) {
    const options: BlpRequestErrorOptions = parseRequestOptions(msg);
    return cacheWrappedError(napiError, new BlpRequestError(msg, options));
  }

  if (msg.includes('Subscription data loss')) {
    return cacheWrappedError(
      napiError,
      new BlpSubscriptionDataLossError(msg, parseSubscriptionDataLossOptions(msg)),
    );
  }

  // Validation errors
  if (
    msg.includes('Invalid argument') ||
    msg.includes('Configuration error') ||
    msg.includes('Operation not found') ||
    msg.includes('Schema element not found') ||
    msg.includes('Schema type mismatch') ||
    msg.includes('Unsupported schema') ||
    msg.includes('invalid extractor') ||
    msg.includes('(did you mean')
  ) {
    const options: BlpValidationErrorOptions = parseValidationOptions(msg);
    return cacheWrappedError(napiError, new BlpValidationError(msg, options));
  }

  // Timeout errors
  if (msg.includes('Request timed out')) {
    return cacheWrappedError(napiError, new BlpTimeoutError(msg, parseRequestOptions(msg)));
  }

  // Internal errors
  if (msg.includes('Internal error') || msg.includes('Channel closed')) {
    return cacheWrappedError(napiError, new BlpInternalError(msg));
  }

  return cacheWrappedError(napiError, new BlpError(msg));
}

function decodeRequestContext(message: string): {
  message: string;
  options: BlpRequestErrorOptions;
} {
  const prefix = '[XBBG_CONTEXT]';
  const newline = message.indexOf('\n');
  if (!message.startsWith(prefix) || newline === -1) {
    return { message, options: {} };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(message.slice(prefix.length, newline));
  } catch {
    return { message, options: {} };
  }
  if (!isPlainObject(parsed)) {
    return { message, options: {} };
  }
  const ids = parsed.bloomberg_request_ids;
  const options: BlpRequestErrorOptions = {
    service: typeof parsed.service === 'string' ? parsed.service : undefined,
    operation: typeof parsed.operation === 'string' ? parsed.operation : undefined,
    request_id: typeof parsed.request_id === 'string' ? parsed.request_id : undefined,
    bloomberg_request_id:
      typeof parsed.bloomberg_request_id === 'string' ? parsed.bloomberg_request_id : undefined,
    bloomberg_request_ids:
      Array.isArray(ids) && ids.every((id): id is string => typeof id === 'string')
        ? ids
        : undefined,
  };
  return { message: message.slice(newline + 1), options };
}

function parseRequestOptions(msg: string): BlpRequestErrorOptions {
  const options: {
    service?: string;
    operation?: string;
    request_id?: string;
    code?: string | number;
  } = {};
  const serviceOpMatch = /on ([^:]+)::([^ (]+)/u.exec(msg);
  if (serviceOpMatch !== null) {
    options.service = serviceOpMatch[1];
    options.operation = serviceOpMatch[2];
  }
  const requestIdMatch = /\[request_id=([^\]]+)\]/u.exec(msg);
  if (requestIdMatch !== null) {
    options.request_id = requestIdMatch[1];
  }
  return options;
}

function parseSubscriptionDataLossOptions(msg: string): BlpSubscriptionDataLossErrorOptions {
  const match = /^Subscription data loss \[topic=(.*)\]: ([\s\S]*)$/u.exec(msg);
  if (match === null) {
    return {};
  }
  return { detail: match[2], topic: match[1] };
}

function parseValidationOptions(msg: string): BlpValidationErrorOptions {
  const options: { element?: string; suggestion?: string } = {};
  const suggestionMatch = /\(did you mean '([^']+)'\?\)/u.exec(msg);
  if (suggestionMatch !== null) {
    options.suggestion = suggestionMatch[1];
  }
  return options;
}
