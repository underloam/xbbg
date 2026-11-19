package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class SessionConnectionsTest {
  @Test
  void keepsServersInConnectionOrderAndIsImmutable() {
    List<String> servers =
        new ArrayList<String>(Arrays.asList("b.example.com:8194", "a.example.com:8194"));
    SessionConnections session = new SessionConnections("request", 1, servers);
    servers.clear();

    assertEquals("request", session.pool());
    assertEquals(1, session.worker());
    assertEquals(Arrays.asList("b.example.com:8194", "a.example.com:8194"), session.servers());
    assertThrows(UnsupportedOperationException.class, () -> session.servers().add("x"));
  }

  @Test
  void comparesByValue() {
    SessionConnections first =
        new SessionConnections("subscription", 0, Collections.singletonList("a.example.com:8194"));
    SessionConnections same =
        new SessionConnections("subscription", 0, Collections.singletonList("a.example.com:8194"));

    assertEquals(first, same);
    assertEquals(first.hashCode(), same.hashCode());
    assertEquals(
        "SessionConnections{pool=subscription, worker=0, servers=[a.example.com:8194]}",
        first.toString());
  }
}
