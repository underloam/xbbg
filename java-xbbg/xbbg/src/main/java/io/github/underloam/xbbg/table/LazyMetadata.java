package io.github.underloam.xbbg.table;

import java.util.Map;

/** Raw schema metadata plus the engine's decoded JSON, decoded on first use. */
final class LazyMetadata {
  private final Map<String, String> raw;
  private final String decodedJson;
  private volatile ResultMetadata decoded;

  LazyMetadata(Map<String, String> raw, String decodedJson) {
    this.raw = raw;
    this.decodedJson = decodedJson;
  }

  Map<String, String> raw() {
    return raw;
  }

  ResultMetadata get() {
    ResultMetadata result = decoded;
    if (result == null) {
      result = ResultMetadata.decode(raw, decodedJson);
      decoded = result;
    }
    return result;
  }
}
