package io.github.underloam.xbbg.sdk;

import io.github.underloam.xbbg.internal.Platform;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Archives built at test time: wheels with RECORD, PEP 503 directories, .conda packages. */
final class SdkFixtures {
  private SdkFixtures() {}

  /** Wheel platform tag and library member for the platform running the tests. */
  static String[] hostWheel() {
    switch (Platform.current()) {
      case WINDOWS_X86_64:
        return new String[] {"py3-none-win_amd64", "blpapi/blpapi3_64.dll"};
      case LINUX_X86_64:
        return new String[] {"py3-none-manylinux2014_x86_64", "blpapi/libblpapi3_64.so"};
      default:
        return new String[] {"py3-none-macosx_11_0_arm64", "blpapi/libblpapi3_64.so"};
    }
  }

  /** conda subdir and library path for the platform running the tests. */
  static String[] hostConda() {
    switch (Platform.current()) {
      case WINDOWS_X86_64:
        return new String[] {"win-64", "Library/bin/blpapi3_64.dll"};
      case LINUX_X86_64:
        return new String[] {"linux-64", "lib/libblpapi3_64.so"};
      default:
        return new String[] {"osx-arm64", "lib/libblpapi3_64.so"};
    }
  }

  /** An auto-flushing UTF-8 {@link PrintStream} over {@code bytes}, to capture CLI output. */
  @SuppressWarnings("JdkObsolete")
  static PrintStream utf8(ByteArrayOutputStream bytes) {
    // Suppressed: the suggested PrintStream(OutputStream, boolean, Charset) is Java 10+, and the
    // tests compile for Java 8.
    try {
      return new PrintStream(bytes, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  static String sha256Hex(byte[] bytes) {
    StringBuilder hex = new StringBuilder();
    for (byte b : sha256(bytes)) {
      hex.append(String.format("%02x", b & 0xff));
    }
    return hex.toString();
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Writes {@code blpapi-<version>-<tag>.whl} into {@code dir}; returns its path. */
  static Path wheel(
      Path dir, String version, String tag, String libMember, byte[] lib, boolean corruptRecord)
      throws IOException {
    String distInfo = "blpapi-" + version + ".dist-info/";
    Map<String, byte[]> members = new LinkedHashMap<>();
    members.put("blpapi/__init__.py", ascii("# blpapi\n"));
    members.put(libMember, lib);
    members.put(distInfo + "METADATA", ascii("Name: blpapi\nVersion: " + version + "\n"));
    members.put(distInfo + "WHEEL", ascii("Wheel-Version: 1.0\n"));
    members.put(distInfo + "licenses/License.txt", ascii("Bloomberg licence\n"));
    StringBuilder record = new StringBuilder();
    for (Map.Entry<String, byte[]> member : members.entrySet()) {
      byte[] digest = sha256(member.getValue());
      if (corruptRecord && member.getKey().equals(libMember)) {
        digest = new byte[32];
      }
      String hash = Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
      record
          .append(member.getKey())
          .append(",sha256=")
          .append(hash)
          .append(',')
          .append(member.getValue().length)
          .append('\n');
    }
    record.append(distInfo).append("RECORD,,\n");
    members.put(distInfo + "RECORD", ascii(record.toString()));
    Files.createDirectories(dir);
    Path wheel = dir.resolve("blpapi-" + version + "-" + tag + ".whl");
    try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(wheel))) {
      for (Map.Entry<String, byte[]> member : members.entrySet()) {
        zip.putNextEntry(new ZipEntry(member.getKey()));
        zip.write(member.getValue());
        zip.closeEntry();
      }
    }
    return wheel;
  }

  /** A PEP 503 directory index: {@code <root>/simple/blpapi/index.html} linking the wheels. */
  static Path simpleIndex(Path root, Path... wheels) throws IOException {
    StringBuilder html = new StringBuilder("<html><body>\n");
    for (Path wheel : wheels) {
      String name = wheel.getFileName().toString();
      html.append("<a href=\"../../files/")
          .append(name)
          .append("#sha256=")
          .append(sha256Hex(Files.readAllBytes(wheel)))
          .append("\">")
          .append(name)
          .append("</a><br/>\n");
    }
    Path project = root.resolve("simple").resolve("blpapi");
    Files.createDirectories(project);
    Files.write(project.resolve("index.html"), ascii(html + "</body></html>\n"));
    return root.resolve("simple");
  }

  /**
   * Writes {@code libblpapi-<version>-h0_0.conda} for {@code subdir}: a stored zip holding
   * zstd-framed tars (raw zstd blocks, so no compressor is needed).
   */
  static Path conda(Path dir, String version, String subdir, String libPath, byte[] lib)
      throws IOException {
    String stem = "libblpapi-" + version + "-h0_0";
    String paths =
        "{\"paths\":[{\"_path\":\""
            + libPath
            + "\",\"path_type\":\"hardlink\","
            + "\"sha256\":\""
            + sha256Hex(lib)
            + "\",\"size_in_bytes\":"
            + lib.length
            + "}],"
            + "\"paths_version\":1}";
    String index =
        "{\"name\":\"libblpapi\",\"version\":\""
            + version
            + "\",\"build\":\"h0_0\","
            + "\"build_number\":0,\"subdir\":\""
            + subdir
            + "\",\"depends\":[]}";
    Map<String, byte[]> info = new LinkedHashMap<>();
    info.put("info/index.json", ascii(index));
    info.put("info/paths.json", ascii(paths));
    info.put("info/licenses/License.txt", ascii("Bloomberg licence\n"));
    Map<String, byte[]> pkg = new LinkedHashMap<>();
    pkg.put(libPath, lib);
    Files.createDirectories(dir);
    Path conda = dir.resolve(stem + ".conda");
    try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(conda))) {
      stored(zip, "metadata.json", ascii("{\"conda_pkg_format_version\": 2}"));
      stored(zip, "pkg-" + stem + ".tar.zst", zstdRaw(tar(pkg)));
      stored(zip, "info-" + stem + ".tar.zst", zstdRaw(tar(info)));
    }
    return conda;
  }

  private static void stored(ZipOutputStream zip, String name, byte[] data) throws IOException {
    ZipEntry entry = new ZipEntry(name);
    CRC32 crc = new CRC32();
    crc.update(data);
    entry.setMethod(ZipEntry.STORED);
    entry.setSize(data.length);
    entry.setCompressedSize(data.length);
    entry.setCrc(crc.getValue());
    zip.putNextEntry(entry);
    zip.write(data);
    zip.closeEntry();
  }

  /** A ustar archive of regular files. */
  private static byte[] tar(Map<String, byte[]> files) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (Map.Entry<String, byte[]> file : files.entrySet()) {
      byte[] header = new byte[512];
      put(header, 0, file.getKey());
      put(header, 100, "0000644");
      put(header, 108, "0000000");
      put(header, 116, "0000000");
      put(header, 124, String.format("%011o", file.getValue().length));
      put(header, 136, String.format("%011o", 1_700_000_000L));
      for (int i = 148; i < 156; i++) {
        header[i] = ' ';
      }
      header[156] = '0';
      put(header, 257, "ustar");
      put(header, 263, "00");
      long sum = 0;
      for (byte b : header) {
        sum += b & 0xff;
      }
      put(header, 148, String.format("%06o", sum));
      header[154] = 0;
      out.write(header);
      out.write(file.getValue());
      int pad = (512 - file.getValue().length % 512) % 512;
      out.write(new byte[pad]);
    }
    out.write(new byte[1024]);
    return out.toByteArray();
  }

  /** One zstd frame of raw (stored) blocks: magic, header, 128 KiB window, no checksum. */
  private static byte[] zstdRaw(byte[] data) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeLe(out, 0xFD2FB528L, 4);
    out.write(0x00);
    out.write(7 << 3);
    int offset = 0;
    do {
      int size = Math.min(128 * 1024, data.length - offset);
      boolean last = offset + size == data.length;
      writeLe(out, ((long) size << 3) | (last ? 1 : 0), 3);
      out.write(data, offset, size);
      offset += size;
    } while (offset < data.length);
    return out.toByteArray();
  }

  private static void writeLe(OutputStream out, long value, int bytes) {
    try {
      for (int i = 0; i < bytes; i++) {
        out.write((int) (value >>> (8 * i)) & 0xff);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void put(byte[] header, int offset, String text) {
    byte[] bytes = ascii(text);
    System.arraycopy(bytes, 0, header, offset, bytes.length);
  }

  private static byte[] ascii(String text) {
    return text.getBytes(StandardCharsets.US_ASCII);
  }
}
