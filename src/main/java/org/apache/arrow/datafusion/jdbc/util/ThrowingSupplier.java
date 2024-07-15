package org.apache.arrow.datafusion.jdbc.util;

public interface ThrowingSupplier<T, E extends Throwable> {
  T get() throws E;
}
