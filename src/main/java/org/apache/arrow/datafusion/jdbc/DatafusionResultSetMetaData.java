package org.apache.arrow.datafusion.jdbc;

import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Map.entry;

public class DatafusionResultSetMetaData implements ResultSetMetaData, GenericWrapper {
  private final String catalog;
  private final String schema;
  private final String table;
  private final List<Field> fields;
  private static final Map<ArrowTypeID, Integer> arrowToSqlType = Map.ofEntries(
      entry(ArrowTypeID.Null, Types.NULL),
      entry(ArrowTypeID.Struct, Types.STRUCT),
      entry(ArrowTypeID.List, Types.ARRAY),
      entry(ArrowTypeID.LargeList, Types.ARRAY),
      entry(ArrowTypeID.FixedSizeList, Types.ARRAY),
      entry(ArrowTypeID.Union, Types.OTHER),
      entry(ArrowTypeID.Map, Types.OTHER),
      entry(ArrowTypeID.Int, Types.INTEGER),
      entry(ArrowTypeID.FloatingPoint, Types.DOUBLE),
      entry(ArrowTypeID.Utf8, Types.VARCHAR),
      entry(ArrowTypeID.LargeUtf8, Types.LONGNVARCHAR),
      entry(ArrowTypeID.Binary, Types.VARBINARY),
      entry(ArrowTypeID.LargeBinary, Types.LONGVARBINARY),
      entry(ArrowTypeID.FixedSizeBinary, Types.BINARY),
      entry(ArrowTypeID.Bool, Types.BOOLEAN),
      entry(ArrowTypeID.Decimal, Types.DECIMAL),
      entry(ArrowTypeID.Date, Types.DATE),
      entry(ArrowTypeID.Time, Types.TIME),
      entry(ArrowTypeID.Timestamp, Types.TIMESTAMP)
//      ArrowTypeID.Interval, Types.OTHER,
//      ArrowTypeID.Duration, Types.OTHER,
//      ArrowTypeID.NONE, Types.OTHER
  );

  private static final Map<Integer, Class<?>> sqlTypeToClass = Map.ofEntries(
      entry(Types.NULL, Object.class),
      entry(Types.STRUCT, Object.class),
      entry(Types.ARRAY, java.sql.Array.class),
      entry(Types.OTHER, Object.class),
      entry(Types.INTEGER, Integer.class),
      entry(Types.DOUBLE, Double.class),
      entry(Types.VARCHAR, String.class),
      entry(Types.LONGNVARCHAR, String.class),
      entry(Types.VARBINARY, byte[].class),
      entry(Types.LONGVARBINARY, byte[].class),
      entry(Types.BINARY, byte[].class),
      entry(Types.BOOLEAN, Boolean.class),
      entry(Types.DECIMAL, Double.class),
      entry(Types.DATE, java.sql.Date.class),
      entry(Types.TIME, java.sql.Time.class),
      entry(Types.TIMESTAMP, java.sql.Timestamp.class)
  );

  public DatafusionResultSetMetaData(String catalog, String schema, String table, List<Field> fields) {
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.fields = fields;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return fields.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return getField(column).isNullable() ? columnNullable : columnNoNulls;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return getField(column).getFieldType().getDictionary().getIndexType().getIsSigned();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getField(column).getName();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return schema;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return table;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return catalog;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return arrowToSqlType.get(getField(column).getFieldType().getType().getTypeID());
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return JDBCType.valueOf(getColumnType(column)).getName();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return sqlTypeToClass.get(getColumnType(column)).getName();
  }

  private Field getField(int column) throws SQLException {
    int index = column - 1;
    if (index < 0 || index >= fields.size()) {
      throw new SQLException(format("Wrong column index %d", column));
    }
    return fields.get(column - 1);
  }
}
