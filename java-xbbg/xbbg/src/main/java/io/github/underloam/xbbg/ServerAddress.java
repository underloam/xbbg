package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.Objects;

/** A Bloomberg server endpoint ({@link EngineConfig.Builder#servers}). */
public final class ServerAddress {
  private final String host;
  private final int port;

  /**
   * Creates an endpoint.
   *
   * @param host the host name or address
   * @param port the TCP port, 0 to 65535
   * @throws IllegalArgumentException if the port is out of range
   */
  public ServerAddress(String host, int port) {
    this.host = Objects.requireNonNull(host, "host");
    this.port = EngineConfig.checkPort("servers.port", port);
  }

  /**
   * Creates an endpoint.
   *
   * @param host the host name or address
   * @param port the TCP port
   * @return the endpoint
   */
  public static ServerAddress of(String host, int port) {
    return new ServerAddress(host, port);
  }

  /**
   * Returns the host.
   *
   * @return the host
   */
  public String host() {
    return host;
  }

  /**
   * Returns the port.
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
    if (!(other instanceof ServerAddress)) {
      return false;
    }
    ServerAddress that = (ServerAddress) other;
    return port == that.port && host.equals(that.host);
  }

  @Override
  public int hashCode() {
    return 31 * host.hashCode() + port;
  }

  @Override
  public String toString() {
    return host + ":" + port;
  }
}
