package io.github.underloam.xbbg.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.Platform;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Downloads from Bloomberg's index and conda-forge; run with {@code -Dxbbg.excludedGroups=
 * -Dgroups=live}.
 */
@Tag("live")
class BlpapiSdkLiveTest {
  private static void provisionTwice(BlpapiSource source, Path cache) throws Exception {
    ProvisionOptions options = ProvisionOptions.builder().source(source).cacheDir(cache).build();
    ProvisionResult first = BlpapiSdk.provision(options).join();
    assertFalse(first.cacheHit());
    assertEquals(Platform.current().id(), first.platform());
    assertTrue(Files.size(first.libraryPath()) > 1_000_000L, first.toString());
    assertFalse(first.licensePaths().isEmpty());
    ProvisionResult again = BlpapiSdk.provision(options).join();
    assertTrue(again.cacheHit());
    assertEquals(first.libraryPath(), again.libraryPath());
  }

  @Test
  void bloombergIndex(@TempDir Path cache) throws Exception {
    provisionTwice(BlpapiSource.pypi(BlpapiSource.BLOOMBERG_PYPI_INDEX), cache);
  }

  @Test
  void condaForge(@TempDir Path cache) throws Exception {
    provisionTwice(BlpapiSource.parse("conda:conda-forge"), cache);
  }
}
