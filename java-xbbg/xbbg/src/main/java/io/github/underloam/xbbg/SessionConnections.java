package io.github.underloam.xbbg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The servers one Bloomberg session is connected to ({@link Engine#connections()}), without
 * credentials or identity details.
 */
public final class SessionConnections {
  private final String pool;
  private final long worker;
  private final List<String> servers;

  /**
   * Creates a value.
   *
   * @param pool the session's pool, {@code "request"} or {@code "subscription"}
   * @param worker the worker id within its pool
   * @param servers the {@code host:port} of each connected server, in the order they connected
   */
  public SessionConnections(String pool, long worker, List<String> servers) {
    this.pool = Objects.requireNonNull(pool, "pool");
    this.worker = worker;
    this.servers = Collections.unmodifiableList(new ArrayList<String>(servers));
  }

  /**
   * Returns the pool the session belongs to.
   *
   * @return {@code "request"} or {@code "subscription"}
   */
  public String pool() {
    return pool;
  }

  /**
   * Returns the worker id within its pool.
   *
   * @return the worker id
   */
  public long worker() {
    return worker;
  }

  /**
   * Returns the servers whose connection is up, as {@code host:port}, in the order they connected.
   * A connection whose status message named no server is not listed.
   *
   * @return an unmodifiable list, empty while the session has no connection
   */
  public List<String> servers() {
    return servers;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SessionConnections)) {
      return false;
    }
    SessionConnections that = (SessionConnections) other;
    return worker == that.worker && pool.equals(that.pool) && servers.equals(that.servers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pool, worker, servers);
  }

  @Override
  public String toString() {
    return "SessionConnections{pool=" + pool + ", worker=" + worker + ", servers=" + servers + "}";
  }
}
