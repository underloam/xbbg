package io.github.underloam.xbbg.subscription;

import java.util.Objects;

/** Availability of one Bloomberg service used by a subscription. */
public final class ServiceStatus {
  private final String service;
  private final boolean up;
  private final long lastChangeUs;

  ServiceStatus(String service, boolean up, long lastChangeUs) {
    this.service = service;
    this.up = up;
    this.lastChangeUs = lastChangeUs;
  }

  /**
   * Returns the service URI.
   *
   * @return the service, such as {@code //blp/mktdata}
   */
  public String service() {
    return service;
  }

  /**
   * Whether the service is available.
   *
   * @return the flag
   */
  public boolean up() {
    return up;
  }

  /**
   * Returns when availability last changed.
   *
   * @return microseconds since the epoch
   */
  public long lastChangeUs() {
    return lastChangeUs;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ServiceStatus)) {
      return false;
    }
    ServiceStatus that = (ServiceStatus) other;
    return service.equals(that.service) && up == that.up && lastChangeUs == that.lastChangeUs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(service, up, lastChangeUs);
  }

  @Override
  public String toString() {
    return "ServiceStatus{service="
        + service
        + ", up="
        + up
        + ", lastChangeUs="
        + lastChangeUs
        + "}";
  }
}
