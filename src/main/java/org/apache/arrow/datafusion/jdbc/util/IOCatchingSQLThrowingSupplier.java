package org.apache.arrow.datafusion.jdbc.util;

import java.io.IOException;
import java.sql.SQLException;

public class IOCatchingSQLThrowingSupplier<T> implements ThrowingSupplier<T, SQLException> {
  private final ThrowingSupplier<T, IOException> valueSupplier;

  public IOCatchingSQLThrowingSupplier(ThrowingSupplier<T, IOException> valueSupplier) {
    this.valueSupplier = valueSupplier;
  }

  @Override
  public T get() throws SQLException {
    try {
      return valueSupplier.get();
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
}
