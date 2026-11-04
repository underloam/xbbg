package io.github.underloam.xbbg;

import io.github.underloam.xbbg.table.DelayedDataInfo;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Delivers {@link XbbgWarning}s. With no listener registered, warnings go to the {@code
 * java.util.logging} logger {@code io.github.underloam.xbbg} at level {@code WARNING}; once a
 * listener is registered, only listeners receive them. A listener that throws is logged and does
 * not affect other listeners or the call that produced the warning.
 */
public final class XbbgWarnings {
  /** Type of the deprecation warning for the {@code ticker} key column. */
  public static final String DEPRECATION_TYPE = "DeprecationWarning";

  /** Code of the deprecation warning for the {@code ticker} key column. */
  public static final String TICKER_KEY_COLUMN_CODE = "XBBG_TICKER_KEY_COLUMN";

  /** Type of delayed-data warnings. */
  public static final String DELAYED_DATA_TYPE = "BlpDelayedDataWarning";

  /** Code of delayed-data warnings for requests. */
  public static final String DELAYED_DATA_CODE = "XBBG_DELAYED_DATA";

  static final String TICKER_KEY_COLUMN_MESSAGE =
      "xbbg results name the security column 'ticker', which collides with Bloomberg's TICKER"
          + " field. The next major release renames it 'security'; set"
          + " EngineConfig.builder().keyColumn(KeyColumn.SECURITY) to switch now.";

  private static final Logger LOG = Logger.getLogger("io.github.underloam.xbbg");
  private static final CopyOnWriteArrayList<Consumer<? super XbbgWarning>> LISTENERS =
      new CopyOnWriteArrayList<Consumer<? super XbbgWarning>>();
  private static final AtomicBoolean TICKER_WARNED = new AtomicBoolean();

  private XbbgWarnings() {}

  /**
   * Registers a listener.
   *
   * @param listener the listener
   */
  public static void addListener(Consumer<? super XbbgWarning> listener) {
    if (listener == null) {
      throw new NullPointerException("listener");
    }
    LISTENERS.add(listener);
  }

  /**
   * Removes a listener registered with {@link #addListener(Consumer)}.
   *
   * @param listener the listener
   * @return {@code true} if it was registered
   */
  public static boolean removeListener(Consumer<? super XbbgWarning> listener) {
    return LISTENERS.remove(listener);
  }

  /**
   * Delivers a warning to the listeners, or to the logger when there are none.
   *
   * @param warning the warning
   */
  public static void emit(XbbgWarning warning) {
    if (warning == null) {
      throw new NullPointerException("warning");
    }
    boolean delivered = false;
    for (Consumer<? super XbbgWarning> listener : LISTENERS) {
      delivered = true;
      try {
        listener.accept(warning);
      } catch (Throwable error) {
        LOG.log(Level.WARNING, "xbbg warning listener failed for " + warning, error);
      }
    }
    if (!delivered && LOG.isLoggable(Level.WARNING)) {
      LOG.log(Level.WARNING, warning.toString());
    }
  }

  /**
   * Emits one {@code BlpDelayedDataWarning} per delayed security, in metadata order: {@code
   * <security>: Bloomberg is delivering delayed data}, plus {@code (<n> minutes behind real time)}
   * when the delay is known.
   *
   * @param delayed security to delayed-data info
   */
  static void emitDelayedData(Map<String, DelayedDataInfo> delayed) {
    for (Map.Entry<String, DelayedDataInfo> entry : delayed.entrySet()) {
      DelayedDataInfo info = entry.getValue();
      if (!info.delayed()) {
        continue;
      }
      String lag =
          info.delayMinutes().isPresent()
              ? " (" + info.delayMinutes().getAsInt() + " minutes behind real time)"
              : "";
      emit(
          new XbbgWarning(
              DELAYED_DATA_TYPE,
              DELAYED_DATA_CODE,
              entry.getKey() + ": Bloomberg is delivering delayed data" + lag,
              null));
    }
  }

  /**
   * Emits the {@code ticker} key-column deprecation warning the first time it applies in this JVM:
   * when the engine's key column is {@code ticker} and a result has a column named {@code ticker}.
   *
   * @param keyColumn the engine's key column
   * @param columnNames the result's column names
   */
  static void warnTickerKeyColumn(KeyColumn keyColumn, Iterable<String> columnNames) {
    if (keyColumn != KeyColumn.TICKER || TICKER_WARNED.get()) {
      return;
    }
    boolean hasTicker = false;
    for (String name : columnNames) {
      if ("ticker".equals(name)) {
        hasTicker = true;
        break;
      }
    }
    if (hasTicker && TICKER_WARNED.compareAndSet(false, true)) {
      emit(
          new XbbgWarning(
              DEPRECATION_TYPE, TICKER_KEY_COLUMN_CODE, TICKER_KEY_COLUMN_MESSAGE, null));
    }
  }

  static void resetTickerKeyColumnWarning() {
    TICKER_WARNED.set(false);
  }
}
