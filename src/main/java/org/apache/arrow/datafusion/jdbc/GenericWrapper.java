package org.apache.arrow.datafusion.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;

public interface GenericWrapper extends Wrapper {
    @Override
    default <T> T unwrap(@SuppressWarnings("SpellCheckingInspection") Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    default boolean isWrapperFor(@SuppressWarnings("SpellCheckingInspection") Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }
}
