// Consumer smoke test: resolves io.github.underloam:xbbg from a file repository the way an
// application would, then runs consumer.Main (../src) on each JDK in xbbgRuntimes.
// See java-xbbg/README.md ("Consumer smoke tests").
import java.time.Duration

plugins {
  java
}

// Artifact under test and the repository holding it: `mvnw deploy` in java-xbbg with
// -DaltDeploymentRepository=id::file:///abs/path/java-xbbg/target/consumer-repo
val xbbgVersion = providers.gradleProperty("xbbgVersion").getOrElse("0.0.0-SNAPSHOT")
val xbbgRepo =
  providers
    .gradleProperty("xbbgRepo")
    .getOrElse(layout.projectDirectory.dir("../../target/consumer-repo").asFile.toURI().toString())
// Runtimes for the smoke task; each needs a toolchain (auto-detected, installations.paths/fromEnv,
// or foojay).
val runtimes =
  providers
    .gradleProperty("xbbgRuntimes")
    .getOrElse("8,11,17,21,25")
    .split(',')
    .map { it.trim() }
    .filter { it.isNotEmpty() }
    .map { it.toInt() }

repositories {
  maven {
    name = "xbbgUnderTest"
    url = uri(xbbgRepo)
  }
}

configurations.configureEach {
  // Each SNAPSHOT deploy replaces the previous one in the file repository: always re-resolve.
  resolutionStrategy.cacheChangingModulesFor(0, "seconds")
}

dependencies {
  implementation("io.github.underloam:xbbg:$xbbgVersion")
}

sourceSets {
  main {
    // consumer.Main is shared with the Maven consumer
    java.setSrcDirs(listOf("../src"))
  }
}

tasks.withType<JavaCompile>().configureEach {
  // Java 8 bytecode from the Gradle JDK, so one compilation runs on every runtime.
  options.release = 8
  options.compilerArgs.add("-Xlint:-options")
}

val smoke =
  tasks.register("smoke") {
    group = "verification"
    description = "Runs consumer.Main on every runtime in xbbgRuntimes (default 8,11,17,21,25)."
  }

runtimes.forEach { runtime ->
  val run =
    tasks.register<JavaExec>("runOn$runtime") {
      group = "verification"
      description = "Runs consumer.Main on Java $runtime."
      classpath = sourceSets.main.get().runtimeClasspath
      mainClass = "consumer.Main"
      javaLauncher = javaToolchains.launcherFor {
        languageVersion = JavaLanguageVersion.of(runtime)
      }
      // JEP 472 (24+): no native-access warning. JDK 8 and 11 accept the flag only together
      // with IgnoreUnrecognizedVMOptions.
      jvmArgs("-XX:+IgnoreUnrecognizedVMOptions", "--enable-native-access=ALL-UNNAMED")
      systemProperty(
        "xbbg.cache.dir",
        layout.buildDirectory.dir("xbbg-cache").get().asFile.absolutePath,
      )
      // -PblpapiArtifact=..., -PexpectVersion=..., -PexpectNativeVersion=... (see consumer.Main)
      listOf("blpapiArtifact", "expectVersion", "expectNativeVersion").forEach { name ->
        providers.gradleProperty(name).orNull?.let { systemProperty("consumer.$name", it) }
      }
      timeout = Duration.ofMinutes(5)
    }
  smoke.configure { dependsOn(run) }
}
