package com.subscriber;

import com.models.PubSubTableInfo;
import com.publisher.ConnectionManager;
import com.subscriber.models.ColumnMetaData;
import com.models.ConnectionInfo;
import com.subscriber.models.TableMetaData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SettingUp implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(SettingUp.class);

    private final List<PubSubTableInfo> pubSubTableInfoList;

    private final ConnectionManager publisherConnectionManager;
    private final ConnectionManager subscriberConnectionManager;

    public SettingUp(ConnectionInfo publisherConnectionInfo,
                     ConnectionInfo subscriberConnectionInfo,
                     List<PubSubTableInfo> pubSubTableInfoList) {
        this.publisherConnectionManager = new ConnectionManager(publisherConnectionInfo.getServer(),
                publisherConnectionInfo.getDatabase(),
                publisherConnectionInfo.getUser(),
                publisherConnectionInfo.getPassword());

        this.subscriberConnectionManager = new ConnectionManager(subscriberConnectionInfo.getServer(),
                subscriberConnectionInfo.getDatabase(),
                subscriberConnectionInfo.getUser(),
                subscriberConnectionInfo.getPassword());

        this.pubSubTableInfoList = pubSubTableInfoList;
    }

    // https://www.baeldung.com/jdbc-database-metadata
    private List<TableMetaData> getTableMetaDataList(List<PubSubTableInfo> pubSubTableInfoList) throws SQLException {
        DatabaseMetaData databaseMetaData = this.publisherConnectionManager.getSQLConnection().getMetaData();
        List<TableMetaData> result = new ArrayList<>();
        for (PubSubTableInfo pubSubTableInfo : pubSubTableInfoList) {
            ResultSet resultSet = databaseMetaData.getTables(null, pubSubTableInfo.getPubSchemaName(), pubSubTableInfo.getPubTableName(), new String[]{"TABLE"});
            if (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                if (pubSubTableInfo.getPubTableName().equalsIgnoreCase(tableName) == true) {
                    ResultSet columns = databaseMetaData.getColumns(null, pubSubTableInfo.getPubSchemaName(), tableName, null);

                    List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        int columnSize = columns.getInt("COLUMN_SIZE");
                        int decimalDigits = columns.getInt("DECIMAL_DIGITS");
                        String typeName = columns.getString("TYPE_NAME");
                        int dataType = columns.getInt("DATA_TYPE");
                        boolean isNullable = columns.getBoolean("IS_NULLABLE");
                        boolean isAutoIncrement = columns.getBoolean("IS_AUTOINCREMENT");
                        columnMetaDataList.add(new ColumnMetaData(columnName, columnSize, decimalDigits, typeName, dataType, isNullable, isAutoIncrement));
                    }

                    TableMetaData tableMetaData = new TableMetaData(tableName, columnMetaDataList);
                    result.add(tableMetaData);
                }
            } else {
                throw new IllegalArgumentException(String.format("could not extract the metadata corresponding to the %s.%s table", pubSubTableInfo.getPubSchemaName(), pubSubTableInfo.getPubTableName()));
            }
        }

        return result;
    }

    private boolean tableExists(String schemaName, String tableName, ConnectionManager connectionManager) throws SQLException {
        String sql = "select exists  (select 1 from information_schema.tables " +
                String.format("where table_schema = '%s' ", schemaName) +
                String.format("and table_name = '%s');", tableName);
        PreparedStatement statement = connectionManager.getSQLConnection().prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        boolean result = false;
        if (resultSet.next()) {
            result = resultSet.getBoolean(1);
        }
        logger.info("{}.{} exists: {}", schemaName, tableName, result);
        return result;
    }

    public void createSubscriberTables() throws ClassNotFoundException, SQLException {
        System.out.println("creating subscriber tables if necessary...");
        logger.info("creating subscriber tables if necessary...");

        this.publisherConnectionManager.createSQLConnection();
        this.subscriberConnectionManager.createSQLConnection();

        List<TableMetaData> tableMetaDataList = this.getTableMetaDataList(this.pubSubTableInfoList);

        String schemaName;
        String tableName;
        TableMetaData tableMetaData;
        for (int i = 0; i < tableMetaDataList.size(); i++) {
            schemaName = this.pubSubTableInfoList.get(i).getSubSchemaName();
            this.createSubscriberSchemaIfNotExists(schemaName, this.subscriberConnectionManager);
            tableName = this.pubSubTableInfoList.get(i).getSubTableName();
            if (this.tableExists(schemaName, tableName, this.subscriberConnectionManager) == false) {
                tableMetaData = tableMetaDataList.get(i);
                this.createSubscriberTable(schemaName, tableName, tableMetaData, this.subscriberConnectionManager);
            } else {
                System.out.printf("the subscriber has the table - '%s.%s' already\r\n", schemaName, tableName);
                logger.info("the subscriber has the table - '{}.{}' already", schemaName, tableName);
            }
        }

        System.out.println("done.");
        logger.info("done.");
    }

    private void createSubscriberSchemaIfNotExists(String schemaName, ConnectionManager connectionManager) throws SQLException {
        String sql = String.format("create schema if not exists %s;", schemaName);

        System.out.printf("trying to create schema '%s' if not exists...\r\n", schemaName);
        logger.info("trying to create schema '{}' if not exists...", schemaName);

        PreparedStatement statement = connectionManager.getSQLConnection().prepareStatement(sql);
        statement.executeUpdate();

        System.out.println("done.");
        logger.info("done.");
    }

    private void createSubscriberTable(String schemaName, String tableName, TableMetaData tableMetaData, ConnectionManager connectionManager) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("create table %s.%s (", schemaName, tableName));
        for (ColumnMetaData columnMetaData : tableMetaData.getColumnMetaDataList()) {
            if (columnMetaData.getTypeName().equals("serial")) {
                sb.append(String.format("%s integer", columnMetaData.getColumnName()));
            } else if (columnMetaData.getTypeName().equals("varchar")) {
                int columnSize = columnMetaData.getColumnSize();
                if (columnSize <= 10485760) {
                    sb.append(String.format("%s varchar(%d)", columnMetaData.getColumnName(), columnSize));
                } else {
                    sb.append(String.format("%s varchar", columnMetaData.getColumnName()));
                }
            } else if (columnMetaData.getTypeName().equals("numeric")) {
                sb.append(String.format("%s numeric(%d, %d)", columnMetaData.getColumnName(), columnMetaData.getColumnSize(), columnMetaData.getDecimalDigits()));
            } else {
                sb.append(String.format("%s %s", columnMetaData.getColumnName(), columnMetaData.getTypeName()));
            }

            if (columnMetaData.isNullable() == false) {
                sb.append(" not null");
            }
            sb.append(",");
        }
        sb.append("action character varying(50),");

        sb.append("tn character varying(150),");
        sb.append("ip character varying(50),");
        sb.append("id integer,");
        sb.append("fn character varying(150),");

        sb.append("timecreated timestamp without time zone NOT NULL DEFAULT now()");
        sb.append(")");
        String sql = sb.toString();

        System.out.printf("trying to create new table: %s\r\n", sql);
        logger.info("trying to create new table: {}", sql);

        PreparedStatement statement = connectionManager.getSQLConnection().prepareStatement(sql);
        statement.executeUpdate();

        System.out.println("done.");
        logger.info("done.");
    }

    public void close() throws SQLException {
        this.publisherConnectionManager.closeSQLConnection();
        this.subscriberConnectionManager.closeSQLConnection();
    }
}
