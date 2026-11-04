package io.github.underloam.xbbg.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * SHA-256 helpers.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class Hashes {
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private Hashes() {}

  /**
   * Creates a SHA-256 digest.
   *
   * @return a new digest
   */
  public static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("the JVM has no SHA-256 implementation", e);
    }
  }

  /**
   * Hashes bytes.
   *
   * @param bytes the bytes
   * @return the lowercase hex digest
   */
  public static String sha256Hex(byte[] bytes) {
    return hex(sha256().digest(bytes));
  }

  /**
   * Hashes a file.
   *
   * @param file the file
   * @return the lowercase hex digest
   * @throws IOException if the file cannot be read
   */
  public static String sha256Hex(Path file) throws IOException {
    MessageDigest digest = sha256();
    byte[] buffer = new byte[64 * 1024];
    try (InputStream in = Files.newInputStream(file)) {
      int read;
      while ((read = in.read(buffer)) > 0) {
        digest.update(buffer, 0, read);
      }
    }
    return hex(digest.digest());
  }

  /**
   * Formats bytes as lowercase hex.
   *
   * @param bytes the bytes
   * @return the hex text
   */
  public static String hex(byte[] bytes) {
    char[] out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      out[i * 2] = HEX[(bytes[i] >> 4) & 0xf];
      out[i * 2 + 1] = HEX[bytes[i] & 0xf];
    }
    return new String(out);
  }
}
