package io.github.underloam.xbbg.subscription;

/**
 * A reusable field name for {@link Tick} lookups. A handle remembers the field's position in the
 * last layout it was used with, so repeated lookups on rows of one layout skip the name lookup.
 * Handles are thread-safe and may be shared.
 */
public final class FieldHandle {
  private final String name;
  private volatile Resolved resolved;

  private FieldHandle(String name) {
    if (name == null) {
      throw new NullPointerException("name");
    }
    this.name = name;
  }

  /**
   * Creates a handle.
   *
   * @param name the field name
   * @return a new handle
   */
  public static FieldHandle of(String name) {
    return new FieldHandle(name);
  }

  /**
   * Returns the field name.
   *
   * @return the name
   */
  public String name() {
    return name;
  }

  int indexIn(TickLayout layout) {
    Resolved current = resolved;
    if (current != null && current.layout == layout) {
      return current.index;
    }
    int index = layout.indexOf(name);
    resolved = new Resolved(layout, index);
    return index;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof FieldHandle && ((FieldHandle) other).name.equals(name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "FieldHandle{" + name + "}";
  }

  private static final class Resolved {
    final TickLayout layout;
    final int index;

    Resolved(TickLayout layout, int index) {
      this.layout = layout;
      this.index = index;
    }
  }
}
