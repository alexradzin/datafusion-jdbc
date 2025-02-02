package org.apache.arrow.datafusion.jdbc;

import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Optional;

import static java.lang.String.format;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;

public class DatafusionStatement implements Statement, GenericWrapper {
  private final Connection connection;
  private final SessionContext context;
  private final BufferAllocator allocator = new RootAllocator();
  private ResultSet resultSet;

  public DatafusionStatement(Connection connection, SessionContext context) {
    this.connection = connection;
    this.context = context;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return Optional.ofNullable(executeImpl(sql)).orElseThrow(() -> new SQLException(format("Query %s did not return result set", sql)));
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    ResultSet rs = executeImpl(sql);
    if (rs == null) {
      return 0;
    }
    ResultSetMetaData md = rs.getMetaData();
    return md.getColumnCount() == 1 && "count".equals(md.getColumnName(1)) && rs.next() ? rs.getInt(1) : 0;
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {

  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {

  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {

  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {

  }

  @Override
  public void cancel() throws SQLException {

  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public void setCursorName(String name) throws SQLException {

  }

  @Override
  @SuppressWarnings("resource")
  public boolean execute(String sql) throws SQLException {
    return executeImpl(sql) != null;
  }

  private ResultSet executeImpl(String sql) {
    return resultSet = context
        .sql(sql)
        .thenComposeAsync(df -> df.collect(allocator))
        .thenApply(this::consume)
        .join();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    try {
      return resultSet;
    } finally {
      resultSet = null;
    }
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {

  }

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {

  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {

  }

  @Override
  public void clearBatch() throws SQLException {

  }

  @Override
  public int[] executeBatch() throws SQLException {
    return new int[0];
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    return 0;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {

  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {

  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  private ResultSet consume(ArrowReader reader) {
    try {
      return new DatafusionResultSet(this, reader);
    } catch (SQLException e) {
      throw new IllegalStateException(e); // TODO: use sneaky throwing instead
    }
  }

}
