package org.apache.arrow.datafusion.jdbc.util;

import java.sql.SQLException;

public class CatchingSQLThrowingSupplier<T> implements ThrowingSupplier<T, SQLException> {
  private final ThrowingSupplier<T, Exception> valueSupplier;

  public CatchingSQLThrowingSupplier(ThrowingSupplier<T, Exception> valueSupplier) {
    this.valueSupplier = valueSupplier;
  }

  @Override
  public T get() throws SQLException {
    try {
      return valueSupplier.get();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
