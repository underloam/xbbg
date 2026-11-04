/**
 * Java 17+ additions to the xbbg API: {@link java.util.concurrent.Flow} publishers over
 * subscription views ({@link io.github.underloam.xbbg.jdk17.XbbgFlow}), sealed types and records
 * for exhaustive pattern matching ({@link io.github.underloam.xbbg.jdk17.DeliveryView}, {@link
 * io.github.underloam.xbbg.jdk17.TickValue}, {@link io.github.underloam.xbbg.jdk17.ErrorKind}),
 * Arrow Java interop ({@link io.github.underloam.xbbg.jdk17.ArrowBackends}, {@link
 * io.github.underloam.xbbg.jdk17.ArrowTables}) and virtual-thread callbacks ({@link
 * io.github.underloam.xbbg.jdk17.VirtualThreads}).
 *
 * <p>Arrow interop is optional: add {@code org.apache.arrow:arrow-vector} and an allocator ({@code
 * arrow-memory-unsafe} or {@code arrow-memory-netty}) to use it, and start the JVM with {@code
 * --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED} (Arrow reads direct
 * buffer addresses through reflection on Java 17+; on the class path the module name is ignored and
 * {@code ALL-UNNAMED} applies). With {@code arrow-memory-netty} also pass {@code
 * -Dio.netty.tryReflectionSetAccessible=true}. On Java 24+ Arrow's memory layer still calls {@code
 * sun.misc.Unsafe} and the JVM prints a deprecation warning (JEP 498); {@code
 * --sun-misc-unsafe-memory-access=allow} silences it. Every other type in this package works
 * without Arrow on the class path.
 */
package io.github.underloam.xbbg.jdk17;
