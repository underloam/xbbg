package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.Objects;
import java.util.Optional;

/**
 * How the engine authorizes its identity ({@link EngineConfig.Builder#auth}). The engine checks
 * that the fields each method needs are non-empty.
 */
public final class AuthConfig {
  private final String method;
  private final String appName;
  private final String dirProperty;
  private final String userId;
  private final String ipAddress;
  private final String token;

  private AuthConfig(
      String method,
      String appName,
      String dirProperty,
      String userId,
      String ipAddress,
      String token) {
    this.method = method;
    this.appName = appName;
    this.dirProperty = dirProperty;
    this.userId = userId;
    this.ipAddress = ipAddress;
    this.token = token;
  }

  /**
   * Authorizes the logged-in Bloomberg user ({@code method: user}).
   *
   * @return the setting
   */
  public static AuthConfig user() {
    return new AuthConfig("user", null, null, null, null, null);
  }

  /**
   * Authorizes an application ({@code method: app}).
   *
   * @param appName the registered application name
   * @return the setting
   */
  public static AuthConfig app(String appName) {
    return new AuthConfig("app", required(appName, "appName"), null, null, null, null);
  }

  /**
   * Authorizes the logged-in user through an application ({@code method: userapp}).
   *
   * @param appName the registered application name
   * @return the setting
   */
  public static AuthConfig userApp(String appName) {
    return new AuthConfig("userapp", required(appName, "appName"), null, null, null, null);
  }

  /**
   * Authorizes a directory-service user ({@code method: directory}).
   *
   * @param dirProperty the directory property identifying the user
   * @return the setting
   */
  public static AuthConfig directory(String dirProperty) {
    return new AuthConfig(
        "directory", null, required(dirProperty, "dirProperty"), null, null, null);
  }

  /**
   * Authorizes a user manually ({@code method: manual}).
   *
   * @param appName the registered application name
   * @param userId the Bloomberg user id
   * @param ipAddress the user's IP address
   * @return the setting
   */
  public static AuthConfig manual(String appName, String userId, String ipAddress) {
    return new AuthConfig(
        "manual",
        required(appName, "appName"),
        null,
        required(userId, "userId"),
        required(ipAddress, "ipAddress"),
        null);
  }

  /**
   * Authorizes with a token ({@code method: token}).
   *
   * @param token the authorization token
   * @return the setting
   */
  public static AuthConfig token(String token) {
    return new AuthConfig("token", null, null, null, null, required(token, "token"));
  }

  /**
   * Returns the method: {@code user}, {@code app}, {@code userapp}, {@code directory}, {@code
   * manual} or {@code token}.
   *
   * @return the method
   */
  public String method() {
    return method;
  }

  /**
   * Returns the application name.
   *
   * @return the name, or empty
   */
  public Optional<String> appName() {
    return Optional.ofNullable(appName);
  }

  /**
   * Returns the directory property.
   *
   * @return the property, or empty
   */
  public Optional<String> dirProperty() {
    return Optional.ofNullable(dirProperty);
  }

  /**
   * Returns the user id.
   *
   * @return the id, or empty
   */
  public Optional<String> userId() {
    return Optional.ofNullable(userId);
  }

  /**
   * Returns the IP address.
   *
   * @return the address, or empty
   */
  public Optional<String> ipAddress() {
    return Optional.ofNullable(ipAddress);
  }

  /**
   * Returns the token.
   *
   * @return the token, or empty
   */
  public Optional<String> token() {
    return Optional.ofNullable(token);
  }

  JsonValue toJson() {
    return Json.object()
        .put("method", method)
        .put("appName", appName)
        .put("dirProperty", dirProperty)
        .put("userId", userId)
        .put("ipAddress", ipAddress)
        .put("token", token)
        .build();
  }

  private static String required(String value, String name) {
    return Objects.requireNonNull(value, name);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof AuthConfig)) {
      return false;
    }
    AuthConfig that = (AuthConfig) other;
    return method.equals(that.method)
        && Objects.equals(appName, that.appName)
        && Objects.equals(dirProperty, that.dirProperty)
        && Objects.equals(userId, that.userId)
        && Objects.equals(ipAddress, that.ipAddress)
        && Objects.equals(token, that.token);
  }

  @Override
  public int hashCode() {
    return Objects.hash(method, appName, dirProperty, userId, ipAddress, token);
  }

  @Override
  public String toString() {
    return "AuthConfig{method="
        + method
        + (appName == null ? "" : ", appName=" + appName)
        + (dirProperty == null ? "" : ", dirProperty=" + dirProperty)
        + (userId == null ? "" : ", userId=" + userId)
        + (ipAddress == null ? "" : ", ipAddress=" + ipAddress)
        + (token == null ? "" : ", token=<redacted>")
        + "}";
  }
}
