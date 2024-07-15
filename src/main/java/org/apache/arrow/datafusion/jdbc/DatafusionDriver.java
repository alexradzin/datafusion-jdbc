package org.apache.arrow.datafusion.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class DatafusionDriver implements Driver {
  private static final String JDBC_URL_PREFIX = "jdbc:datafusion:";
  static {
    try {
      DriverManager.registerDriver(new DatafusionDriver());
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return new DatafusionConnection(url.substring(JDBC_URL_PREFIX.length()));
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(JDBC_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}
