package org.apache.arrow.datafusion.jdbc;

import org.apache.arrow.datafusion.jdbc.util.IOCatchingSQLThrowingSupplier;
import org.apache.arrow.datafusion.jdbc.util.ThrowingSupplier;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.Map.entry;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class DatafusionResultSet implements ResultSet, GenericWrapper {
  private final VectorSchemaRoot root;
  private Boolean hasNextBatch;
  private int rowsCount;
  private int rowIndex = -1;
  private final Statement statement;
  private final ThrowingSupplier<Boolean, SQLException> nextBatchLoader;
  private final ThrowingSupplier<Void, SQLException> closer;
  private final Map<String, Entry<Field, Integer>> fields;
  private final ResultSetMetaData resultSetMetaData;

  private static final Map<Class<?>, Object> defaultValues = Map.of(boolean.class, false, short.class, (short)0, int.class, 0,  long.class, 0L, float.class, 0.0f, double.class, 0.0);
  private static final Set<Class<?>> intTypes = Set.of(byte.class, short.class, int.class, long.class, Byte.class, Short.class, Integer.class, Long.class);
  private static final Set<Class<?>> floatingTypes = Set.of(float.class, double.class, Float.class, Double.class);
  private static final Map<Class<?>, Function<Number, ?>> numberTransformers = Map.of(
      Byte.class, Number::byteValue,
      Short.class, Number::shortValue,
      Integer.class, Number::intValue,
      Long.class, Number::longValue,
      Float.class, Number::floatValue,
      Double.class, Number::doubleValue,
      BigDecimal.class, n -> {
        Class<?> clazz = n.getClass();
        if (intTypes.contains(clazz)) {
          return BigDecimal.valueOf(n.longValue());
        }
        if (floatingTypes.contains(clazz)) {
          return BigDecimal.valueOf(n.doubleValue());
        }
        throw new IllegalArgumentException(format("Cannot convert %s to BigDecimal", clazz));
      },
      Boolean.class, n -> {
        Class<?> clazz = n.getClass();
        if (intTypes.contains(clazz)) {
          return n.longValue() != 0;
        }
        if (floatingTypes.contains(clazz)) {
          return n.doubleValue() != 0.0;
        }
        throw new IllegalArgumentException(format("Cannot convert %s to Boolean", clazz));

      }
  );
  private static final Map<Class<?>, Function<String, ?>> parsers = Map.ofEntries(
      Map.entry(Boolean.class, Boolean::parseBoolean),
      Map.entry(Byte.class, (Function<String, Object>) Byte::parseByte),
      Map.entry(Short.class, (Function<String, Object>) Short::parseShort),
      Map.entry(Integer.class, (Function<String, Object>) Integer::parseInt),
      Map.entry(Long.class, (Function<String, Object>) Long::parseLong),
      Map.entry(Float.class, Float::parseFloat),
      Map.entry(Double.class, Double::parseDouble),
      Map.entry(BigDecimal.class, (Function<String, Object>) s -> BigDecimal.valueOf(Double.parseDouble(s))),
      Map.entry(byte[].class, (Function<String, Object>) String::getBytes),
      Map.entry(Date.class, s -> (Function<String, Object>) Date::valueOf),
      Map.entry(Time.class, Time::valueOf)
  );

  public DatafusionResultSet(Statement statement, ArrowReader reader) throws SQLException {
    this.statement = statement;
    nextBatchLoader = new IOCatchingSQLThrowingSupplier<>(reader::loadNextBatch);
    root = new IOCatchingSQLThrowingSupplier<>(reader::getVectorSchemaRoot).get();
    closer = new IOCatchingSQLThrowingSupplier<>(() -> {
      reader.close();
      return null;
    });

    List<Field> fieldsList = root.getSchema().getFields();
    resultSetMetaData = new DatafusionResultSetMetaData("", "", "", fieldsList);
    fields = IntStream.range(0, fieldsList.size()).boxed().map(i -> entry(fieldsList.get(i), i)).collect(toUnmodifiableMap(p -> p.getKey().getName(), p -> p));
  }

  @Override
  public boolean next() throws SQLException {
    if (hasNextBatch != null) {
      rowIndex++;
    }
    if (hasNextBatch == null || rowIndex >= rowsCount) {
      hasNextBatch = nextBatchLoader.get();
      if (!hasNextBatch) {
        rowIndex = -1;
        return false;
      }
      rowsCount = root.getRowCount();
      rowIndex = 0;
    }
    return true;
  }

  @Override
  public void close() throws SQLException {
    root.close();
    closer.get();
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return "";
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return getObject(columnIndex, boolean.class);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return getObject(columnIndex, byte.class);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return getObject(columnIndex, short.class);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return getObject(columnIndex, int.class);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return getObject(columnIndex, long.class);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return getObject(columnIndex, float.class);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return getObject(columnIndex, double.class);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return getBigDecimal(columnIndex).setScale(scale, RoundingMode.CEILING);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return getObject(columnIndex, byte[].class);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return getObject(columnIndex, Date.class);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return getObject(columnIndex, Time.class);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getObject(columnIndex, Timestamp.class);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getAsciiStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getUnicodeStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(findColumn(columnLabel));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public String getCursorName() throws SQLException {
    return "";
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return resultSetMetaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    int index = columnIndex - 1;
    if (index < 0 || index > fields.size()) {
      throw new SQLException(format("Wrong column index %d", columnIndex));
    }
    return root.getVector(index).getObject(rowIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return ofNullable(fields.get(columnLabel)).map(Entry::getValue).orElseThrow(() -> new SQLException(format("Unknown column label %s", columnLabel)));
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isLast() throws SQLException {
    return false;
  }

  @Override
  public void beforeFirst() throws SQLException {

  }

  @Override
  public void afterLast() throws SQLException {

  }

  @Override
  public boolean first() throws SQLException {
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    return 0;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {

  }

  @Override
  public int getFetchDirection() throws SQLException {
    return FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {

  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    return TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {

  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {

  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {

  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {

  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {

  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {

  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {

  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {

  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {

  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {

  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {

  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {

  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {

  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {

  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {

  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {

  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {

  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {

  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {

  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {

  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {

  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {

  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {

  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {

  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {

  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {

  }

  @Override
  public void insertRow() throws SQLException {

  }

  @Override
  public void updateRow() throws SQLException {

  }

  @Override
  public void deleteRow() throws SQLException {

  }

  @Override
  public void refreshRow() throws SQLException {

  }

  @Override
  public void cancelRowUpdates() throws SQLException {

  }

  @Override
  public void moveToInsertRow() throws SQLException {

  }

  @Override
  public void moveToCurrentRow() throws SQLException {

  }

  @Override
  public Statement getStatement() throws SQLException {
    return statement;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return getRef(findColumn(columnLabel));
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return getBlob(findColumn(columnLabel));
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return getClob(findColumn(columnLabel));
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return getArray(findColumn(columnLabel));
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getTime(findColumn(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getTimestamp(findColumn(columnLabel), cal);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    try {
      String spec = getString(columnIndex);
      return spec == null ? null : new URI(spec).toURL();
    } catch (URISyntaxException | MalformedURLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return getURL(findColumn(columnLabel));
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {

  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {

  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {

  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {

  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return getRowId(findColumn(columnLabel));
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {

  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {
    return HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {

  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return getNClob(findColumn(columnLabel));
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return getSQLXML(findColumn(columnLabel));
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return getString(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return getNString(findColumn(columnLabel));
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return getNCharacterStream(findColumn(columnLabel));
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    Object value = getObject(columnIndex);
    if (value == null) {
      return (T)defaultValues.get(type);
    }
    Class<?> clazz = value.getClass();
    if (Text.class.equals(clazz)) {
      value = value.toString();
      clazz = String.class;
    }
    if (type.equals(clazz) || type.isAssignableFrom(clazz)) {
      return (T)value;
    }
    if (String.class.equals(type)) {
      return (T)value.toString();
    }
    if (value instanceof Number) {
      return (T)numberTransformers.get(type).apply((Number)value);
    }
    if (LocalDateTime.class.equals(clazz)) {
      LocalDateTime ldt = (LocalDateTime)value;
      java.util.Date date = java.util.Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
      long millis = date.getTime();
      if (java.util.Date.class.equals(type)) {
        return (T)date;
      }
      if (Date.class.equals(type)) {
        return (T)new Date(millis);
      }
      if (Time.class.equals(type)) {
        return (T)new Time(millis);
      }
      if (Timestamp.class.equals(type)) {
        return (T)new Timestamp(millis);
      }
    }
    if (value instanceof String) {
      return (T)parsers.get(type).apply((String)value);
    }

    throw new SQLException(format("Cannot transform %s to %s", clazz, type));
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(findColumn(columnLabel), type);
  }
}
