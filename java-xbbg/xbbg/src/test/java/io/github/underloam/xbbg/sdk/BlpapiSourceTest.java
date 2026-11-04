package io.github.underloam.xbbg.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class BlpapiSourceTest {
  private static final String HASH =
      "623458f7738766fbb188fff7cae688b3adf6bc18f9ec9a1b0c9d046f21147d8e";

  @Test
  void prefixedSpecsParseAndRoundTrip() {
    for (String spec :
        Arrays.asList(
            "pypi:" + BlpapiSource.BLOOMBERG_PYPI_INDEX,
            "pypi:https://mirror.example/simple/#blpapi-mirror",
            "conda:conda-forge",
            "conda:https://conda.anaconda.org/conda-forge#blpapi",
            "artifact:C:\\wheels\\blpapi-3.26.9.1-py3-none-win_amd64.whl",
            "artifact:https://host/x.conda#sha256=" + HASH)) {
      BlpapiSource source = BlpapiSource.parse(spec);
      assertEquals(spec, source.spec(), spec);
      assertEquals(source, BlpapiSource.parse(source.spec()));
      assertEquals(spec, source.toString(), "no credentials, nothing to redact");
    }
  }

  @Test
  void toStringRedactsCredentialsWhileSpecKeepsThem() {
    String spec = "pypi:https://user:p@ss@mirror.example/simple/#blpapi-mirror";
    BlpapiSource source = BlpapiSource.parse(spec);
    assertEquals(spec, source.spec());
    assertEquals(source, BlpapiSource.parse(source.spec()));
    assertEquals("pypi:https://***@mirror.example/simple/#blpapi-mirror", source.toString());
    assertEquals(
        "artifact:https://***@h/x.whl?sig=1#sha256=" + HASH,
        BlpapiSource.artifact("https://token@h/x.whl?sig=1#sha256=" + HASH).toString());
    assertEquals(
        "conda:https://conda.example/t/abc/channel",
        BlpapiSource.conda("https://conda.example/t/abc/channel").toString(),
        "only user-info is redacted here");
  }

  @Test
  void defaultNamesAreImpliedAndOmitted() {
    BlpapiSource pypi = BlpapiSource.parse("pypi:https://idx/simple/");
    assertEquals(BlpapiSource.Kind.PYPI, pypi.kind());
    assertEquals("blpapi", pypi.name().get());
    assertEquals(BlpapiSource.pypi("https://idx/simple/"), pypi);
    BlpapiSource conda = BlpapiSource.parse("CONDA: conda-forge ");
    assertEquals("libblpapi", conda.name().get());
    assertEquals("conda:conda-forge", conda.spec());
    assertEquals("mirror", BlpapiSource.parse("conda:conda-forge#mirror").name().get());
  }

  @Test
  void bareLocationsAreClassifiedByShape() {
    assertEquals(BlpapiSource.Kind.ARTIFACT, BlpapiSource.parse("C:\\dl\\blpapi.WHL").kind());
    assertEquals(
        BlpapiSource.Kind.ARTIFACT,
        BlpapiSource.parse("https://h/libblpapi-3.26.9.1-h0_0.tar.bz2?x=1").kind());
    assertEquals(BlpapiSource.Kind.ARTIFACT, BlpapiSource.parse("file:///tmp/a.conda").kind());
    assertEquals(
        BlpapiSource.Kind.PYPI, BlpapiSource.parse(BlpapiSource.BLOOMBERG_PYPI_INDEX).kind());
    assertEquals(BlpapiSource.Kind.PYPI, BlpapiSource.parse("D:\\mirror\\simple").kind());
    assertEquals(BlpapiSource.Kind.CONDA, BlpapiSource.parse("conda-forge").kind());
    assertEquals(
        BlpapiSource.Kind.CONDA,
        BlpapiSource.parse("https://conda.anaconda.org/conda-forge").kind());
  }

  @Test
  void artifactHashesAreSplitNormalizedAndValidated() {
    BlpapiSource fromFragment =
        BlpapiSource.artifact("https://h/x.whl#SHA256=" + HASH.toUpperCase(java.util.Locale.ROOT));
    assertEquals("https://h/x.whl", fromFragment.location());
    assertEquals(HASH, fromFragment.sha256().get());
    assertEquals(fromFragment, BlpapiSource.artifact("https://h/x.whl", HASH));
    assertFalse(BlpapiSource.artifact("/tmp/x.whl").sha256().isPresent());
    assertThrows(IllegalArgumentException.class, () -> BlpapiSource.artifact("x.whl", "abc"));
    assertThrows(
        IllegalArgumentException.class,
        () -> BlpapiSource.artifact("https://h/x.whl#sha256=" + HASH.replace('a', 'g')));
  }

  @Test
  void blankInputsAreRejectedWithTheirName() {
    IllegalArgumentException empty =
        assertThrows(IllegalArgumentException.class, () -> BlpapiSource.parse("  "));
    assertTrue(empty.getMessage().contains("spec"));
    assertThrows(IllegalArgumentException.class, () -> BlpapiSource.parse("pypi:"));
    assertThrows(IllegalArgumentException.class, () -> BlpapiSource.parse("conda:#pkg"));
    assertThrows(IllegalArgumentException.class, () -> BlpapiSource.parse("artifact: "));
    assertThrows(IllegalArgumentException.class, () -> BlpapiSource.pypi("https://i/", " "));
    assertThrows(IllegalArgumentException.class, () -> BlpapiSource.conda(null));
  }
}
