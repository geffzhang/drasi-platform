package io.drasi;

import io.drasi.source.sdk.BootstrapStream;
import io.drasi.source.sdk.SourceProxy;
import io.drasi.source.sdk.models.BootstrapRequest;
import io.drasi.source.sdk.models.SourceElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.*;

public class ResultStream implements BootstrapStream {

    private static final Logger log = LoggerFactory.getLogger(ResultStream.class);
    private final BootstrapRequest request;
    private final Connection connection;

    private final Queue<TableCursor> cursors;

    public ResultStream(BootstrapRequest request) {
        try {
            this.connection = getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.request = request;
        this.cursors = new LinkedList<>();

        for (var table : request.getNodeLabels()) {
            cursors.add(new TableCursor(table));
        }
    }

    public SourceElement next() {
        var cursor = cursors.peek();
        if (cursor == null)
            return null;

        var next = cursor.next(connection);
        while (next == null) {
            cursors.poll().close();
            cursor = cursors.peek();
            if (cursor == null)
                return null;

            next = cursor.next(connection);
        }
        
        return next;
    }

    @Override
    public void close() {
        log.info("Closing ResultStream");
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Connection getConnection() throws SQLException {
        switch (SourceProxy.GetConfigValue("connector")) {
            case "PostgreSQL":
                var propsPG = new Properties();
                propsPG.setProperty("user", SourceProxy.GetConfigValue("user"));
                propsPG.setProperty("password", SourceProxy.GetConfigValue("password"));
                propsPG.setProperty("sslmode", SourceProxy.GetConfigValue("sslMode", "prefer"));

                return DriverManager.getConnection("jdbc:postgresql://" + SourceProxy.GetConfigValue("host") + ":" + SourceProxy.GetConfigValue("port") + "/" + SourceProxy.GetConfigValue("database"), propsPG);
            case "MySQL":
                var propsMySql = new Properties();
                propsMySql.setProperty("user", SourceProxy.GetConfigValue("user"));
                propsMySql.setProperty("password", SourceProxy.GetConfigValue("password"));
                propsMySql.setProperty("sslmode", SourceProxy.GetConfigValue("sslMode", "prefer"));

                var jdbcConnectionString = "jdbc:mysql://" + SourceProxy.GetConfigValue("host") + ":" + SourceProxy.GetConfigValue("port") + "/" + SourceProxy.GetConfigValue("database");

                return DriverManager.getConnection(jdbcConnectionString, propsMySql);
            case "SQLServer":
                var propsSQL = new Properties();
                String sqlUser = SourceProxy.GetConfigValue("user");
                String sqlPassword = SourceProxy.GetConfigValue("password");
                if (sqlUser != null) {
                    propsSQL.setProperty("user", sqlUser);
                }
                if (sqlPassword != null) {
                    propsSQL.setProperty("password", sqlPassword);
                }
                propsSQL.setProperty("encrypt", SourceProxy.GetConfigValue("encrypt"));
                propsSQL.setProperty("trustServerCertificate", SourceProxy.GetConfigValue("trustServerCertificate", "false"));
                propsSQL.setProperty("authentication", SourceProxy.GetConfigValue("authentication", "NotSpecified"));

                return DriverManager.getConnection("jdbc:sqlserver://"  + SourceProxy.GetConfigValue("host") + ":" + SourceProxy.GetConfigValue("port") + ";databaseName=" + SourceProxy.GetConfigValue("database"), propsSQL);
            case "Oracle":
                var propsOracle = new Properties();
                propsOracle.setProperty("user", SourceProxy.GetConfigValue("user"));
                propsOracle.setProperty("password", SourceProxy.GetConfigValue("password"));
                
                // Oracle JDBC URL format: jdbc:oracle:thin:@host:port:SID
                // or jdbc:oracle:thin:@//host:port/service_name
                String connectionFormat = SourceProxy.GetConfigValue("connectionFormat", "SID");
                String jdbcUrl;
                
                if ("SERVICE".equalsIgnoreCase(connectionFormat)) {
                    jdbcUrl = "jdbc:oracle:thin:@//" + SourceProxy.GetConfigValue("host") + ":" + 
                              SourceProxy.GetConfigValue("port") + "/" + SourceProxy.GetConfigValue("database");
                } else {
                    // Default to SID format
                    jdbcUrl = "jdbc:oracle:thin:@" + SourceProxy.GetConfigValue("host") + ":" + 
                              SourceProxy.GetConfigValue("port") + ":" + SourceProxy.GetConfigValue("database");
                }
                
                return DriverManager.getConnection(jdbcUrl, propsOracle);
            default:
                throw new IllegalArgumentException("Unknown connector");
        }
    }

    @Override
    public List<String> validate() {
        var result = new ArrayList<String>();
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            String connector = SourceProxy.GetConfigValue("connector");
            
            request.getNodeLabels().forEach(table -> {
                try {
                    ResultSet tables;
                    
                    if (connector.equalsIgnoreCase("Oracle")) {
                        // For Oracle, handle schema.table format and uppercase table names
                        String schemaPattern = null;
                        String tableNamePattern = table;
                        
                        if (table.contains(".")) {
                            String[] parts = table.split("\\.");
                            schemaPattern = parts[0];
                            tableNamePattern = parts[1];
                        } else {
                            // In Oracle, if no schema is specified, use the current user's schema
                            schemaPattern = SourceProxy.GetConfigValue("user").toUpperCase();
                        }
                        
                        // Oracle table names are typically stored in uppercase
                        tables = metaData.getTables(null, schemaPattern, tableNamePattern.toUpperCase(), new String[]{"TABLE"});
                    } else {
                        tables = metaData.getTables(null, null, table, new String[]{"TABLE"});
                    }
                    
                    if (!tables.next()) {
                        result.add("Table " + table + " not found");
                    }
                } catch (SQLException e) {
                    result.add(e.getMessage());
                }
            });
        }
        catch (SQLException e) {
            return List.of(e.getMessage());
        }
        return result;
    }
}
