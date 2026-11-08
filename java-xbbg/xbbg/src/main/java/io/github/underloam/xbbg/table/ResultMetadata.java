package io.github.underloam.xbbg.table;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.internal.JsonFields;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Result metadata attached to every request and recipe result: the raw Arrow schema metadata plus
 * its decoded parts (Bloomberg request ids, entitlement ids, delayed-data flags, security errors,
 * field exceptions, field errors and type misses). The engine decodes the metadata; absent parts
 * are empty. When Bloomberg's field-exception or field-error metadata is malformed, only reading
 * that part fails.
 */
public final class ResultMetadata {
  private static final ResultMetadata EMPTY =
      new ResultMetadata(
          Collections.<String, String>emptyMap(),
          null,
          Collections.<String>emptyList(),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);

  private final Map<String, String> metadata;
  private final String bloombergRequestId;
  private final List<String> bloombergRequestIds;
  private final Map<String, List<Integer>> eidData;
  private final Map<String, DelayedDataInfo> delayed;
  private final Map<String, BloombergMetadataError> securityErrors;
  private final Map<String, List<BloombergFieldException>> fieldExceptions;
  private final Map<String, BloombergFieldError> fieldErrors;
  private final Map<String, Long> typeMisses;
  private final String fieldExceptionsError;
  private final String fieldErrorsError;

  private ResultMetadata(
      Map<String, String> metadata,
      String bloombergRequestId,
      List<String> bloombergRequestIds,
      Map<String, List<Integer>> eidData,
      Map<String, DelayedDataInfo> delayed,
      Map<String, BloombergMetadataError> securityErrors,
      Map<String, List<BloombergFieldException>> fieldExceptions,
      Map<String, BloombergFieldError> fieldErrors,
      Map<String, Long> typeMisses,
      String fieldExceptionsError,
      String fieldErrorsError) {
    this.metadata = Collections.unmodifiableMap(metadata);
    this.bloombergRequestId = bloombergRequestId;
    this.bloombergRequestIds = Collections.unmodifiableList(bloombergRequestIds);
    this.eidData = eidData;
    this.delayed = delayed;
    this.securityErrors = securityErrors;
    this.fieldExceptions = fieldExceptions;
    this.fieldErrors = fieldErrors;
    this.typeMisses = typeMisses;
    this.fieldExceptionsError = fieldExceptionsError;
    this.fieldErrorsError = fieldErrorsError;
  }

  /**
   * Returns metadata with no entries.
   *
   * @return the empty metadata
   */
  public static ResultMetadata empty() {
    return EMPTY;
  }

  /**
   * Builds the metadata from the raw schema metadata and the engine's decoded JSON.
   *
   * @param metadata the raw schema metadata
   * @param decodedJson the engine's decoded metadata JSON, or {@code null} when nothing was decoded
   * @return the metadata
   * @throws io.github.underloam.xbbg.BlpInternalException if the decoded JSON is malformed
   */
  public static ResultMetadata decode(Map<String, String> metadata, String decodedJson) {
    Map<String, String> raw = new LinkedHashMap<String, String>(metadata);
    if (decodedJson == null) {
      return raw.isEmpty()
          ? EMPTY
          : new ResultMetadata(
              raw,
              null,
              Collections.<String>emptyList(),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
    }
    Map<String, Object> root =
        JsonFields.object(JsonFields.parse(decodedJson, "result metadata"), "result metadata");
    String singleId = JsonFields.optString(root, "bloombergRequestId");
    List<String> ids = JsonFields.optStrings(root, "bloombergRequestIds");
    return new ResultMetadata(
        raw,
        singleId,
        ids == null ? Collections.<String>emptyList() : ids,
        decodeEids(JsonFields.optObj(root, "eidData")),
        decodeDelayed(JsonFields.optObj(root, "delayed")),
        decodeSecurityErrors(JsonFields.optObj(root, "securityErrors")),
        decodeFieldExceptions(JsonFields.optObj(root, "fieldExceptions")),
        decodeFieldErrors(JsonFields.optObj(root, "fieldErrors")),
        decodeTypeMisses(JsonFields.optObj(root, "typeMisses")),
        JsonFields.optString(root, "fieldExceptionsError"),
        JsonFields.optString(root, "fieldErrorsError"));
  }

  /**
   * Returns the raw Arrow schema metadata.
   *
   * @return an unmodifiable map, in schema order
   */
  public Map<String, String> metadata() {
    return metadata;
  }

  /**
   * Returns the Bloomberg support request id of a result produced by one request.
   *
   * @return the id, or empty (always empty for results composed from several requests)
   */
  public Optional<String> bloombergRequestId() {
    return Optional.ofNullable(bloombergRequestId);
  }

  /**
   * Returns every Bloomberg support request id, sorted and deduplicated, including the singular
   * case.
   *
   * @return the ids, possibly empty
   */
  public List<String> bloombergRequestIds() {
    return bloombergRequestIds;
  }

  /**
   * Returns the entitlement ids per security (requested with {@code returnEids}).
   *
   * @return security to EIDs, or empty when the result carries none
   */
  public Optional<Map<String, List<Integer>>> eidData() {
    return Optional.ofNullable(eidData);
  }

  /**
   * Returns the delayed-data flags per security. Present only when an {@code onDelayed} check ran;
   * securities Bloomberg returned no flag for are absent.
   *
   * @return security to delayed-data info, or empty
   */
  public Optional<Map<String, DelayedDataInfo>> delayed() {
    return Optional.ofNullable(delayed);
  }

  /**
   * Returns the Bloomberg errors per security (requested with {@code includeSecurityErrors}).
   *
   * @return security to error, or empty
   */
  public Optional<Map<String, BloombergMetadataError>> securityErrors() {
    return Optional.ofNullable(securityErrors);
  }

  /**
   * Returns the field exceptions per security.
   *
   * @return security to field exceptions, or empty
   * @throws BlpInternalException if Bloomberg's {@code xbbg.field_exceptions} metadata is malformed
   */
  public Optional<Map<String, List<BloombergFieldException>>> fieldExceptions() {
    if (fieldExceptionsError != null) {
      throw new BlpInternalException(fieldExceptionsError);
    }
    return Optional.ofNullable(fieldExceptions);
  }

  /**
   * Returns the refused field-info ids, keyed by the server's echoed id.
   *
   * @return field id to error, or empty
   * @throws BlpInternalException if Bloomberg's {@code xbbg.field_errors} metadata is malformed
   */
  public Optional<Map<String, BloombergFieldError>> fieldErrors() {
    if (fieldErrorsError != null) {
      throw new BlpInternalException(fieldErrorsError);
    }
    return Optional.ofNullable(fieldErrors);
  }

  /**
   * Returns how many values per field did not match the field's resolved type.
   *
   * @return field to count, or empty
   */
  public Optional<Map<String, Long>> typeMisses() {
    return Optional.ofNullable(typeMisses);
  }

  private static Map<String, List<Integer>> decodeEids(Map<String, Object> object) {
    if (object == null) {
      return null;
    }
    Map<String, List<Integer>> result = new LinkedHashMap<String, List<Integer>>();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      List<Object> values = JsonFields.array(entry.getValue(), "eidData." + entry.getKey());
      List<Integer> eids = new ArrayList<Integer>(values.size());
      for (Object value : values) {
        long eid = JsonFields.longOf(value, "eidData." + entry.getKey());
        if (eid < Integer.MIN_VALUE || eid > Integer.MAX_VALUE) {
          throw JsonFields.malformed("eidData." + entry.getKey(), "is out of the 32-bit range");
        }
        eids.add(Integer.valueOf((int) eid));
      }
      result.put(entry.getKey(), Collections.unmodifiableList(eids));
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<String, DelayedDataInfo> decodeDelayed(Map<String, Object> object) {
    if (object == null) {
      return null;
    }
    Map<String, DelayedDataInfo> result = new LinkedHashMap<String, DelayedDataInfo>();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      Map<String, Object> info = JsonFields.object(entry.getValue(), "delayed." + entry.getKey());
      Long minutes = JsonFields.optLong(info, "delayMinutes");
      if (minutes != null && (minutes < Integer.MIN_VALUE || minutes > Integer.MAX_VALUE)) {
        throw JsonFields.malformed("delayed." + entry.getKey(), "delayMinutes is out of range");
      }
      result.put(
          entry.getKey(),
          new DelayedDataInfo(
              JsonFields.bool(info, "delayed"),
              minutes == null ? null : Integer.valueOf(minutes.intValue())));
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<String, BloombergMetadataError> decodeSecurityErrors(
      Map<String, Object> object) {
    if (object == null) {
      return null;
    }
    Map<String, BloombergMetadataError> result =
        new LinkedHashMap<String, BloombergMetadataError>();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      Map<String, Object> error =
          JsonFields.object(entry.getValue(), "securityErrors." + entry.getKey());
      result.put(
          entry.getKey(),
          new BloombergMetadataError(
              JsonFields.optString(error, "category"),
              codeText(error, "securityErrors." + entry.getKey()),
              JsonFields.optString(error, "subcategory"),
              JsonFields.optString(error, "message")));
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<String, List<BloombergFieldException>> decodeFieldExceptions(
      Map<String, Object> object) {
    if (object == null) {
      return null;
    }
    Map<String, List<BloombergFieldException>> result =
        new LinkedHashMap<String, List<BloombergFieldException>>();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      String what = "fieldExceptions." + entry.getKey();
      List<Object> items = JsonFields.array(entry.getValue(), what);
      List<BloombergFieldException> exceptions =
          new ArrayList<BloombergFieldException>(items.size());
      for (Object item : items) {
        Map<String, Object> exception = JsonFields.object(item, what);
        exceptions.add(
            new BloombergFieldException(
                JsonFields.optString(exception, "field"),
                JsonFields.optString(exception, "category"),
                codeText(exception, what),
                JsonFields.optString(exception, "subcategory"),
                JsonFields.optString(exception, "message")));
      }
      result.put(entry.getKey(), Collections.unmodifiableList(exceptions));
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<String, BloombergFieldError> decodeFieldErrors(Map<String, Object> object) {
    if (object == null) {
      return null;
    }
    Map<String, BloombergFieldError> result = new LinkedHashMap<String, BloombergFieldError>();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      Map<String, Object> error =
          JsonFields.object(entry.getValue(), "fieldErrors." + entry.getKey());
      result.put(
          entry.getKey(),
          new BloombergFieldError(
              JsonFields.string(error, "category"),
              JsonFields.intValue(error, "code"),
              JsonFields.string(error, "subcategory"),
              JsonFields.string(error, "message"),
              JsonFields.optString(error, "source")));
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<String, Long> decodeTypeMisses(Map<String, Object> object) {
    if (object == null) {
      return null;
    }
    Map<String, Long> result = new LinkedHashMap<String, Long>();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      result.put(
          entry.getKey(),
          Long.valueOf(JsonFields.longOf(entry.getValue(), "typeMisses." + entry.getKey())));
    }
    return Collections.unmodifiableMap(result);
  }

  private static String codeText(Map<String, Object> object, String what) {
    Object code = object.get("code");
    if (code == null || code instanceof String) {
      return (String) code;
    }
    if (code instanceof Number && !(code instanceof Double)) {
      return code.toString();
    }
    throw JsonFields.malformed(what + ".code", "must be a string or an integer");
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ResultMetadata)) {
      return false;
    }
    ResultMetadata that = (ResultMetadata) other;
    return metadata.equals(that.metadata)
        && Objects.equals(bloombergRequestId, that.bloombergRequestId)
        && bloombergRequestIds.equals(that.bloombergRequestIds)
        && Objects.equals(eidData, that.eidData)
        && Objects.equals(delayed, that.delayed)
        && Objects.equals(securityErrors, that.securityErrors)
        && Objects.equals(fieldExceptions, that.fieldExceptions)
        && Objects.equals(fieldErrors, that.fieldErrors)
        && Objects.equals(typeMisses, that.typeMisses)
        && Objects.equals(fieldExceptionsError, that.fieldExceptionsError)
        && Objects.equals(fieldErrorsError, that.fieldErrorsError);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        metadata,
        bloombergRequestId,
        bloombergRequestIds,
        eidData,
        delayed,
        securityErrors,
        fieldExceptions,
        fieldErrors,
        typeMisses,
        fieldExceptionsError,
        fieldErrorsError);
  }

  @Override
  public String toString() {
    return "ResultMetadata{bloombergRequestIds="
        + bloombergRequestIds
        + ", eidData="
        + eidData
        + ", delayed="
        + delayed
        + ", securityErrors="
        + securityErrors
        + ", fieldExceptions="
        + (fieldExceptionsError != null ? "<" + fieldExceptionsError + ">" : fieldExceptions)
        + ", fieldErrors="
        + (fieldErrorsError != null ? "<" + fieldErrorsError + ">" : fieldErrors)
        + ", typeMisses="
        + typeMisses
        + ", metadataKeys="
        + metadata.keySet()
        + "}";
  }
}
