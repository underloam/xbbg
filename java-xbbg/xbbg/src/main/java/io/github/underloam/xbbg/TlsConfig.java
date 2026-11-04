package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * TLS settings ({@link EngineConfig.Builder#tls}). Client credentials and trust material must both
 * be given, and both as paths or both as bytes; the engine checks this when connecting. The
 * password defaults to empty. {@link #toString()} never shows the password, paths or bytes.
 */
public final class TlsConfig {
  private final TlsMaterial clientCredentials;
  private final String clientCredentialsPassword;
  private final TlsMaterial trustMaterial;
  private final Integer handshakeTimeoutMs;
  private final Integer crlFetchTimeoutMs;

  private TlsConfig(Builder builder) {
    this.clientCredentials = builder.clientCredentials;
    this.clientCredentialsPassword = builder.clientCredentialsPassword;
    this.trustMaterial = builder.trustMaterial;
    this.handshakeTimeoutMs = builder.handshakeTimeoutMs;
    this.crlFetchTimeoutMs = builder.crlFetchTimeoutMs;
  }

  /**
   * Starts a TLS configuration.
   *
   * @return a builder with every field omitted
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the client credentials (PKCS#12).
   *
   * @return the material, or empty
   */
  public Optional<TlsMaterial> clientCredentials() {
    return Optional.ofNullable(clientCredentials);
  }

  /**
   * Returns the client credentials password.
   *
   * @return the password, or empty
   */
  public Optional<String> clientCredentialsPassword() {
    return Optional.ofNullable(clientCredentialsPassword);
  }

  /**
   * Returns the trusted certificates (PKCS#7).
   *
   * @return the material, or empty
   */
  public Optional<TlsMaterial> trustMaterial() {
    return Optional.ofNullable(trustMaterial);
  }

  /**
   * Returns the TLS handshake timeout.
   *
   * @return milliseconds, or empty for the SDK default
   */
  public OptionalInt handshakeTimeoutMs() {
    return handshakeTimeoutMs == null ? OptionalInt.empty() : OptionalInt.of(handshakeTimeoutMs);
  }

  /**
   * Returns the certificate revocation list fetch timeout.
   *
   * @return milliseconds, or empty for the SDK default
   */
  public OptionalInt crlFetchTimeoutMs() {
    return crlFetchTimeoutMs == null ? OptionalInt.empty() : OptionalInt.of(crlFetchTimeoutMs);
  }

  JsonValue toJson() {
    JsonObjectBuilder json = Json.object();
    json.put("clientCredentials", clientCredentials == null ? null : clientCredentials.toJson());
    json.put("clientCredentialsPassword", clientCredentialsPassword);
    json.put("trustMaterial", trustMaterial == null ? null : trustMaterial.toJson());
    json.put("handshakeTimeoutMs", handshakeTimeoutMs);
    json.put("crlFetchTimeoutMs", crlFetchTimeoutMs);
    return json.build();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TlsConfig)) {
      return false;
    }
    TlsConfig that = (TlsConfig) other;
    return Objects.equals(clientCredentials, that.clientCredentials)
        && Objects.equals(clientCredentialsPassword, that.clientCredentialsPassword)
        && Objects.equals(trustMaterial, that.trustMaterial)
        && Objects.equals(handshakeTimeoutMs, that.handshakeTimeoutMs)
        && Objects.equals(crlFetchTimeoutMs, that.crlFetchTimeoutMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        clientCredentials,
        clientCredentialsPassword,
        trustMaterial,
        handshakeTimeoutMs,
        crlFetchTimeoutMs);
  }

  @Override
  public String toString() {
    return "TlsConfig{clientCredentials="
        + clientCredentials
        + ", clientCredentialsPassword="
        + (clientCredentialsPassword == null ? "null" : "<redacted>")
        + ", trustMaterial="
        + trustMaterial
        + ", handshakeTimeoutMs="
        + handshakeTimeoutMs
        + ", crlFetchTimeoutMs="
        + crlFetchTimeoutMs
        + "}";
  }

  /** Builds a {@link TlsConfig}. */
  public static final class Builder {
    private TlsMaterial clientCredentials;
    private String clientCredentialsPassword;
    private TlsMaterial trustMaterial;
    private Integer handshakeTimeoutMs;
    private Integer crlFetchTimeoutMs;

    private Builder() {}

    /**
     * Sets the client credentials (PKCS#12).
     *
     * @param value the material
     * @return this builder
     */
    public Builder clientCredentials(TlsMaterial value) {
      this.clientCredentials = value;
      return this;
    }

    /**
     * Sets the client credentials password.
     *
     * @param value the password
     * @return this builder
     */
    public Builder clientCredentialsPassword(String value) {
      this.clientCredentialsPassword = value;
      return this;
    }

    /**
     * Sets the trusted certificates (PKCS#7).
     *
     * @param value the material
     * @return this builder
     */
    public Builder trustMaterial(TlsMaterial value) {
      this.trustMaterial = value;
      return this;
    }

    /**
     * Sets the TLS handshake timeout; the engine rejects negative values.
     *
     * @param value milliseconds
     * @return this builder
     */
    public Builder handshakeTimeoutMs(int value) {
      this.handshakeTimeoutMs = value;
      return this;
    }

    /**
     * Sets the certificate revocation list fetch timeout; the engine rejects negative values.
     *
     * @param value milliseconds
     * @return this builder
     */
    public Builder crlFetchTimeoutMs(int value) {
      this.crlFetchTimeoutMs = value;
      return this;
    }

    /**
     * Builds the configuration.
     *
     * @return the configuration
     */
    public TlsConfig build() {
      return new TlsConfig(this);
    }
  }
}
