package io.github.underloam.xbbg.table;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

/**
 * The {@code ROWS} backend result: one unmodifiable map per row (column name to value, in column
 * order, boxed as {@link Column#get(int)} does) plus the result metadata. The list itself is
 * unmodifiable.
 */
public final class RowsResult extends AbstractList<Map<String, Object>>
    implements RandomAccess, ResultMetadataView {
  private final List<Map<String, Object>> rows;
  private final List<String> columnNames;
  private final LazyMetadata metadata;

  private RowsResult(
      List<Map<String, Object>> rows, List<String> columnNames, LazyMetadata metadata) {
    this.rows = Collections.unmodifiableList(rows);
    this.columnNames = Collections.unmodifiableList(columnNames);
    this.metadata = metadata;
  }

  /**
   * Materializes the rows of a table.
   *
   * @param table the table
   * @return the rows with the table's metadata
   */
  public static RowsResult of(Table table) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(table.numRows());
    for (Row row : table) {
      rows.add(row.toMap());
    }
    return new RowsResult(rows, table.columnNames(), table.lazyMetadata());
  }

  /**
   * Returns the column names in schema order.
   *
   * @return an unmodifiable list
   */
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  public Map<String, Object> get(int index) {
    return rows.get(index);
  }

  @Override
  public int size() {
    return rows.size();
  }

  @Override
  public Map<String, String> metadata() {
    return metadata.raw();
  }

  @Override
  public ResultMetadata resultMetadata() {
    return metadata.get();
  }
}
