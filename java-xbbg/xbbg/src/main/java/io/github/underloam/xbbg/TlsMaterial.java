package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonValue;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/**
 * TLS client credentials (PKCS#12) or trust material (PKCS#7): a file path, or DER bytes that are
 * copied and never written to disk. {@link #toString()} shows neither the path nor the bytes.
 */
public final class TlsMaterial {
  private final String path;
  private final byte[] bytes;

  private TlsMaterial(String path, byte[] bytes) {
    this.path = path;
    this.bytes = bytes;
  }

  /**
   * Material read from a file.
   *
   * @param path the file
   * @return the material
   */
  public static TlsMaterial path(Path path) {
    return new TlsMaterial(Objects.requireNonNull(path, "path").toString(), null);
  }

  /**
   * Material read from a file.
   *
   * @param path the file name
   * @return the material
   */
  public static TlsMaterial path(String path) {
    return new TlsMaterial(Objects.requireNonNull(path, "path"), null);
  }

  /**
   * DER-encoded material; the array is copied.
   *
   * @param bytes the DER bytes
   * @return the material
   */
  public static TlsMaterial bytes(byte[] bytes) {
    return new TlsMaterial(null, Objects.requireNonNull(bytes, "bytes").clone());
  }

  /**
   * DER-encoded material; the buffer's remaining bytes are copied without moving its position.
   *
   * @param bytes the DER bytes
   * @return the material
   */
  public static TlsMaterial bytes(ByteBuffer bytes) {
    ByteBuffer source = Objects.requireNonNull(bytes, "bytes").duplicate();
    byte[] copy = new byte[source.remaining()];
    source.get(copy);
    return new TlsMaterial(null, copy);
  }

  /**
   * Returns the file path of path material.
   *
   * @return the path, or empty for byte material
   */
  public Optional<String> path() {
    return Optional.ofNullable(path);
  }

  /**
   * Returns a copy of the bytes of byte material.
   *
   * @return the bytes, or empty for path material
   */
  public Optional<byte[]> bytes() {
    return bytes == null ? Optional.<byte[]>empty() : Optional.of(bytes.clone());
  }

  /**
   * Whether this is path material.
   *
   * @return {@code true} for a path
   */
  public boolean isPath() {
    return path != null;
  }

  JsonValue toJson() {
    return path != null
        ? Json.object().put("path", path).build()
        : Json.object().put("bytes", Base64.getEncoder().encodeToString(bytes)).build();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TlsMaterial)) {
      return false;
    }
    TlsMaterial that = (TlsMaterial) other;
    return Objects.equals(path, that.path) && Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return 31 * Objects.hashCode(path) + Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
    return path != null ? "TlsMaterial{path}" : "TlsMaterial{" + bytes.length + " bytes}";
  }
}
