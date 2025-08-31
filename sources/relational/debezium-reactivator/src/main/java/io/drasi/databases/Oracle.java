/*
* Copyright 2024 The Drasi Authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package io.drasi.databases;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConnection;
import io.drasi.DatabaseStrategy;
import io.drasi.models.NodeMapping;
import io.drasi.models.RelationalGraphMapping;
import io.drasi.source.sdk.Reactivator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

public class Oracle implements DatabaseStrategy {
    private static final Logger log = LoggerFactory.getLogger(Oracle.class);

    @Override
    public JdbcConnection getConnection(Configuration config) {
        var oracleConfig = new OracleConnectorConfig(config);
        var jdbcConfig = oracleConfig.getJdbcConfig();
        
        var connection = new OracleConnection(jdbcConfig, "drasi");
        return connection;
    }

    @Override
    public NodeMapping getNodeMapping(Connection conn, String schema, String tableName) throws SQLException {
        // Oracle metadata typically stores unquoted identifiers in UPPERCASE.
        // Attempt with provided names first; if not found and provided names are not uppercased, retry with uppercase.
        SQLException lastEx = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            String attemptSchema = schema;
            String attemptTable = tableName;
            if (attempt == 1) {
                boolean needsRetry = false;
                if (schema != null && !schema.equals(schema.toUpperCase())) {
                    attemptSchema = schema.toUpperCase();
                    needsRetry = true;
                }
                if (!tableName.equals(tableName.toUpperCase())) {
                    attemptTable = tableName.toUpperCase();
                    needsRetry = true;
                }
                if (!needsRetry) break; // No transformation, avoid duplicate attempt
            }

            try (var rs = conn.getMetaData().getPrimaryKeys(null, attemptSchema, attemptTable)) {
                if (rs.next()) {
                    var mapping = new NodeMapping();
                    var schemaName = rs.getString("TABLE_SCHEM");
                    mapping.tableName = schemaName + "." + attemptTable;
                    mapping.keyField = rs.getString("COLUMN_NAME");
                    mapping.labels = Collections.singleton(tableName); // Preserve original label casing
                    return mapping;
                }
            } catch (SQLException e) {
                lastEx = e;
            }
        }
        if (lastEx != null) throw lastEx;
        throw new SQLException("No primary key found for " + tableName + " (after case normalization attempts)");
    }

    @Override
    public long extractLsn(JsonNode sourceChange) {
    // Prefer commit_scn (transaction commit order) then fallback to scn
    long commitScn = sourceChange.path("commit_scn").asLong(0);
    if (commitScn > 0) return commitScn;
    return sourceChange.path("scn").asLong(0);
    }

    @Override
    public String extractTableName(JsonNode sourceChange) {
        var schema = sourceChange.path("schema").asText();
        var table = sourceChange.path("table").asText();
        return schema + "." + table;
    }

    @Override
    public String getDatabaseNameConfigName() {
        return "database.dbname";
    }

    @Override
    public String getTablesListConfigName() {
        return "table.include.list";
    }

    @Override
    public Configuration createConnectorConfig(Configuration baseConfig) {
        var logicalName = baseConfig.getString("name");
        return Configuration.create()
                // Start with the base configuration.
                .with(baseConfig)
                // Specify the Oracle connector class.
                .with("connector.class", "io.debezium.connector.oracle.OracleConnector")
                // Set Oracle specific configurations
                .with("database.connection.adapter", "logminer")
                // If started first time, start from beginning, else start from last stored SCN.
                .with("snapshot.mode", "initial")
                // Set Oracle LogMiner specific configurations
                .with("log.mining.strategy", "online_catalog")
                .with("log.mining.transaction.retention.ms", "172800000") // 48 hours
                // Recommended tuning defaults (can be overridden via source configuration):
                .with("max.queue.size", "2048")
                .with("max.batch.size", "1024")
                .build();
    }

    @Override
    public void initialize(Configuration config, RelationalGraphMapping relationalGraphMapping) {
        // Oracle doesn't require special initialization like PostgreSQL's publications
        log.info("Oracle connector initialized");
    }
}