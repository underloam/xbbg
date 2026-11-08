package io.github.underloam.xbbg;

/**
 * Static Bloomberg functions on the configured engine: {@code Blp.bdp(...)}, {@code Blp.bdh(...)},
 * {@code Blp.subscribe(...)} and the rest. Configure the engine with {@link
 * Xbbg#configure(EngineConfig)} (otherwise {@code localhost:8194} is used); it connects on first
 * use and is shared by every call.
 */
public final class Blp extends BlpSubscriptions {
  private Blp() {}
}
