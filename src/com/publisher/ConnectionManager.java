package com.publisher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {
    private static final Logger logger = LogManager.getLogger(ConnectionManager.class);

    private final String server;
    private final String database;
    private final String user;
    private final String password;

    private Connection sqlConnection;
    private Connection replicationConnection;

    public ConnectionManager(String server, String database, String user, String password) {
        this.server = server;
        this.database = database;
        this.user = user;
        this.password = password;

        logger.info("server: {}, database: {}, user: {}", server, database, user);
    }

    // https://jdbc.postgresql.org/documentation/head/replication.html
    public void createReplicationConnection() throws ClassNotFoundException, SQLException {
        String url = "jdbc:postgresql://" + this.server + "/" + this.database;
        Properties props = new Properties();
        PGProperty.USER.set(props, this.user);
        PGProperty.PASSWORD.set(props, this.password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        Class.forName("org.postgresql.Driver");
        this.replicationConnection = DriverManager.getConnection(url, props);
    }

    public Connection getReplicationConnection() {
        return this.replicationConnection;
    }

    public void closeReplicationConnection() throws SQLException {
        logger.info("trying to close the replication connection ...");
        this.replicationConnection.close();
        logger.info("done.");
    }

    public void createSQLConnection() throws ClassNotFoundException, SQLException {
        String url = "jdbc:postgresql://" + this.server + "/" + this.database;
        Properties props = new Properties();
        props.setProperty("user", this.user);
        props.setProperty("password", this.password);
        Class.forName("org.postgresql.Driver");
        this.sqlConnection = DriverManager.getConnection(url, props);
        this.sqlConnection.setAutoCommit(true);
    }

    public Connection getSQLConnection() {
        return this.sqlConnection;
    }

    public void closeSQLConnection() throws SQLException {
        logger.info("trying to close the connection ...");
        this.sqlConnection.close();
        logger.info("done.");
    }
}
