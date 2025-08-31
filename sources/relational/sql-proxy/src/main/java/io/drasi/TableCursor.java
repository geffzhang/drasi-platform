package io.drasi;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.drasi.source.sdk.SourceProxy;
import io.drasi.source.sdk.models.SourceElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

class TableCursor {
    private static final Logger log = LoggerFactory.getLogger(TableCursor.class);
    public final String tableName;
    public NodeMapping mapping;
    public ResultSet resultSet;
    public ResultSetMetaData metaData;
    public int columnCount;

    public TableCursor(String tableName) {
        this.tableName = tableName;
    }

    private void Init(Connection connection) throws SQLException {
        if (resultSet == null) {
            mapping = ReadMappingFromSchema(tableName, connection);
            var statement = connection.createStatement();

            String connector = SourceProxy.GetConfigValue("connector");
            String quote;
            if (connector.equalsIgnoreCase("MySQL")) {
                quote = "`";
            } else if (connector.equalsIgnoreCase("Oracle")) {
                quote = "\"";
            } else {
                quote = "\"";
            }
            var sanitizedTableName = tableName.replace(quote, "").replace(";", "");
            
            // Oracle specific handling for table names
            if (connector.equalsIgnoreCase("Oracle")) {
                // For Oracle, we need to handle schema.table format
                if (sanitizedTableName.contains(".")) {
                    String[] parts = sanitizedTableName.split("\\.");
                    String schema = parts[0];
                    String table = parts[1];
                    resultSet = statement.executeQuery("SELECT * FROM " + quote + schema + quote + "." + quote + table + quote);
                } else {
                    resultSet = statement.executeQuery("SELECT * FROM " + quote + sanitizedTableName + quote);
                }
            } else {
                resultSet = statement.executeQuery("SELECT * FROM " + quote + sanitizedTableName + quote);
            }
            metaData = resultSet.getMetaData();
            columnCount = metaData.getColumnCount();
        }
    }

    public SourceElement next(Connection connection) {
        try {
            Init(connection);

            if (resultSet.next()) {
                var properties = JsonNodeFactory.instance.objectNode();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    int columnType = metaData.getColumnType(i);
                    PutField(columnName, columnType, resultSet, i, properties);
                }

                if (!properties.has(mapping.keyField)) {
                    return null;
                }

                var nodeId = SanitizeNodeId(mapping.tableName + ":" + properties.path(mapping.keyField).asText());

                return new SourceElement(nodeId, properties, mapping.labels);
            }
        }
        catch (SQLException e) {
            log.error("Error reading from database", e);
            throw new RuntimeException(e);
        }

        return null;
    }

    public void close() {
        try {
            if (resultSet != null)
                resultSet.close();
        } catch (SQLException e) {
            log.error("Error closing result set", e);
        }
    }

    private String SanitizeNodeId(String nodeId) {
        return nodeId.replace('.', ':');
    }

    private NodeMapping ReadMappingFromSchema(String table, Connection connection) throws SQLException {
        var metadata = connection.getMetaData();
        table = table.trim();
        String schemaName = null;
        String tableName = table;

        if (table.contains(".")) {
            var tableComps = table.split("\\.");
            schemaName = tableComps[0];
            tableName = tableComps[1];
        }

        String connector = SourceProxy.GetConfigValue("connector");
        ResultSet rs;
        
        // Oracle requires special handling for schema names
        if (connector.equalsIgnoreCase("Oracle")) {
            // In Oracle, schema name is typically the user name (in uppercase)
            if (schemaName == null) {
                schemaName = SourceProxy.GetConfigValue("user").toUpperCase();
            }
            rs = metadata.getPrimaryKeys(null, schemaName, tableName.toUpperCase());
        } else {
            rs = metadata.getPrimaryKeys(null, schemaName, tableName);
        }
        
        if (!rs.next())
            throw new SQLException("No primary key found for " + table);
        
        var mapping = new NodeMapping();
        
        if (connector.equalsIgnoreCase("Oracle")) {
            // For Oracle, we need to handle schema name differently
            mapping.tableName = (schemaName != null ? schemaName : rs.getString("TABLE_SCHEM")) + "." + tableName;
        } else {
            mapping.tableName = rs.getString("TABLE_SCHEM") + "." + tableName;
        }
        mapping.keyField = rs.getString("COLUMN_NAME");
        mapping.labels = Collections.singleton(tableName);

        return mapping;
    }

    private void PutField(String columnName, int columnType, ResultSet rs, int columnIndex, ObjectNode output) throws SQLException {
        switch (columnType) {
            case Types.TIMESTAMP:
                Timestamp sqlTimestamp = rs.getTimestamp(columnIndex);
                if (sqlTimestamp != null) {
                    LocalDateTime localDateTime = sqlTimestamp.toLocalDateTime();
                    output.put(columnName, localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                } else {
                    output.putNull(columnName);
                }
                break;
            case Types.INTEGER:
                int intValue = rs.getInt(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, intValue);
                }
                break;
            case Types.BIGINT:
                long longValue = rs.getLong(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, longValue);
                }
                break;
            case Types.DOUBLE:
                double doubleValue = rs.getDouble(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, doubleValue);
                }
                break;
            case Types.FLOAT:
                float floatValue = rs.getFloat(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, floatValue);
                }
                break;
            case Types.BOOLEAN:
                boolean booleanValue = rs.getBoolean(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, booleanValue);
                }
                break;
            case Types.SMALLINT:
                short shortValue = rs.getShort(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, shortValue);
                }
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:  // Oracle often uses DECIMAL type
                BigDecimal bigDecimalValue = rs.getBigDecimal(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, bigDecimalValue);
                }
                break;
            case Types.DATE:  // Handle Oracle DATE type which includes time component
                java.sql.Date dateValue = rs.getDate(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, dateValue.toString());
                }
                break;
            case Types.CLOB:  // Handle Oracle CLOB type
                Clob clob = rs.getClob(columnIndex);
                if (rs.wasNull() || clob == null) {
                    output.putNull(columnName);
                } else {
                    // Read up to 32KB from CLOB to avoid excessive memory usage
                    long length = Math.min(clob.length(), 32768);
                    output.put(columnName, clob.getSubString(1, (int)length));
                }
                break;
            case Types.BLOB:  // Handle Oracle BLOB type - convert to Base64 string
                Blob blob = rs.getBlob(columnIndex);
                if (rs.wasNull() || blob == null) {
                    output.putNull(columnName);
                } else {
                    // For BLOBs, we'll just indicate it's binary data rather than loading it
                    output.put(columnName, "[BINARY DATA]");
                }
                break;
            case Types.NULL:
                output.putNull(columnName);
                break;
            default: // Handle other types as strings
                String value = rs.getString(columnIndex);
                if (rs.wasNull()) {
                    output.putNull(columnName);
                } else {
                    output.put(columnName, value);
                }
        }
    }
}
