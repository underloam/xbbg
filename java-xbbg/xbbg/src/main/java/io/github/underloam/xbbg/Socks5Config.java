package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.Objects;

/** A SOCKS5 proxy applied to every server ({@link EngineConfig.Builder#socks5}). */
public final class Socks5Config {
  private final String host;
  private final int port;

  /**
   * Creates a proxy setting.
   *
   * @param host the proxy host
   * @param port the proxy port, 0 to 65535
   * @throws IllegalArgumentException if the port is out of range
   */
  public Socks5Config(String host, int port) {
    this.host = Objects.requireNonNull(host, "host");
    this.port = EngineConfig.checkPort("socks5.port", port);
  }

  /**
   * Creates a proxy setting.
   *
   * @param host the proxy host
   * @param port the proxy port
   * @return the setting
   */
  public static Socks5Config of(String host, int port) {
    return new Socks5Config(host, port);
  }

  /**
   * Returns the proxy host.
   *
   * @return the host
   */
  public String host() {
    return host;
  }

  /**
   * Returns the proxy port.
   *
   * @return the port
   */
  public int port() {
    return port;
  }

  JsonValue toJson() {
    return Json.object().put("host", host).put("port", port).build();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Socks5Config)) {
      return false;
    }
    Socks5Config that = (Socks5Config) other;
    return port == that.port && host.equals(that.host);
  }

  @Override
  public int hashCode() {
    return 31 * host.hashCode() + port;
  }

  @Override
  public String toString() {
    return "socks5://" + host + ":" + port;
  }
}
