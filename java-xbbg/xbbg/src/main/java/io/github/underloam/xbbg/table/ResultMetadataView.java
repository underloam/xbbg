package io.github.underloam.xbbg.table;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A result that carries {@link ResultMetadata}: {@link Table} and {@link RowsResult}. The default
 * methods delegate to {@link #resultMetadata()}.
 */
public interface ResultMetadataView {
  /**
   * Returns the decoded result metadata.
   *
   * @return the metadata
   */
  ResultMetadata resultMetadata();

  /**
   * Returns the raw Arrow schema metadata.
   *
   * @return an unmodifiable map
   */
  default Map<String, String> metadata() {
    return resultMetadata().metadata();
  }

  /**
   * Returns the Bloomberg support request id of a result produced by one request.
   *
   * @return the id, or empty
   */
  default Optional<String> bloombergRequestId() {
    return resultMetadata().bloombergRequestId();
  }

  /**
   * Returns every Bloomberg support request id.
   *
   * @return the ids, possibly empty
   */
  default List<String> bloombergRequestIds() {
    return resultMetadata().bloombergRequestIds();
  }

  /**
   * Returns the entitlement ids per security.
   *
   * @return security to EIDs, or empty
   */
  default Optional<Map<String, List<Integer>>> eidData() {
    return resultMetadata().eidData();
  }

  /**
   * Returns the delayed-data flags per security.
   *
   * @return security to delayed-data info, or empty
   */
  default Optional<Map<String, DelayedDataInfo>> delayed() {
    return resultMetadata().delayed();
  }

  /**
   * Returns the Bloomberg errors per security.
   *
   * @return security to error, or empty
   */
  default Optional<Map<String, BloombergMetadataError>> securityErrors() {
    return resultMetadata().securityErrors();
  }

  /**
   * Returns the field exceptions per security.
   *
   * @return security to field exceptions, or empty
   */
  default Optional<Map<String, List<BloombergFieldException>>> fieldExceptions() {
    return resultMetadata().fieldExceptions();
  }

  /**
   * Returns the refused field-info ids.
   *
   * @return field id to error, or empty
   */
  default Optional<Map<String, BloombergFieldError>> fieldErrors() {
    return resultMetadata().fieldErrors();
  }

  /**
   * Returns the per-field type-miss counts.
   *
   * @return field to count, or empty
   */
  default Optional<Map<String, Long>> typeMisses() {
    return resultMetadata().typeMisses();
  }
}
