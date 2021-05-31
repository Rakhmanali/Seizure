package com.publisher;

import com.models.ConnectionInfo;
import com.publisher.models.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.PGConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;

/*

    Replication is the process of copying data from a central
    database to one or more databases.

 */

public class Replication implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(Replication.class);

    private final String publication;
    private final String slot;
    private Stream stream;
    private final ConnectionManager connectionManager;
    private final Decode decode;

    public Replication(ConnectionInfo connectionInfo, String publication, String slot)
            throws ClassNotFoundException, SQLException {

        this.publication = publication;
        this.slot = slot;

        this.connectionManager = new ConnectionManager(connectionInfo.getServer(),
                connectionInfo.getDatabase(),
                connectionInfo.getUser(),
                connectionInfo.getPassword());

        this.connectionManager.createSQLConnection();
        this.connectionManager.createReplicationConnection();

        this.decode = new Decode();
        this.decode.loadDataTypes(this.connectionManager.getSQLConnection());
    }

    public void initializeReplication(boolean dropSlotIfExists) throws SQLException {
        PreparedStatement statement = this.connectionManager.getSQLConnection().prepareStatement(
                "select 1 from pg_catalog.pg_replication_slots WHERE slot_name = ?");

        statement.setString(1, this.slot);
        ResultSet resultSet = statement.executeQuery();

        if (resultSet.next()) {
            if (dropSlotIfExists) {
                this.dropReplicationSlot();
                this.createReplicationSlot();
            }
        } else {
            this.createReplicationSlot();
        }
    }

    public void createReplicationSlot() throws SQLException {
        PGConnection pgConnection = this.connectionManager.getReplicationConnection().unwrap(PGConnection.class);
        // More details about pgoutput options in PostgreSQL project:
        // https://github.com/postgres, source file:
        // postgres/src/backend/replication/pgoutput/pgoutput.c
        pgConnection.getReplicationAPI().createReplicationSlot().logical().withSlotName(this.slot)
                .withOutputPlugin("pgoutput").make();
    }

    public void dropReplicationSlot() throws SQLException {
        logger.info("trying to drop the replication slot: {}...", this.slot);
        PGConnection pgConnection = this.connectionManager.getReplicationConnection().unwrap(PGConnection.class);
        pgConnection.getReplicationAPI().dropReplicationSlot(this.slot);
        logger.info("done.");
    }

    public Event readEvent(boolean isSimpleEvent, boolean withBeginCommit, String outputFormat, Long startLSN) {
        Event event = null;
        try {
            if (this.stream == null) {
                Connection replicationConnection = this.connectionManager.getReplicationConnection();
                this.stream = new Stream(this.decode, replicationConnection, this.publication, this.slot, startLSN);
            }
            event = this.stream.readStream(isSimpleEvent, withBeginCommit, outputFormat);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("readEvent() - {}", ex.toString());
        }

        return event;
    }

    public void close() {

        try {
            logger.info("trying to close connections...");
            this.connectionManager.closeSQLConnection();
            this.connectionManager.closeReplicationConnection();
            logger.info("done");

            this.connectionManager.createReplicationConnection();
            this.dropReplicationSlot();
            this.connectionManager.closeReplicationConnection();

            System.out.printf("please, do not forget to be sure that the replication slot: %s was dropped\r\n", this.slot);
            logger.info("please, do not forget to be sure that the replication slot: {} was dropped", this.slot);
        } catch (Exception ex) {
            logger.error("close() - {}", ex.toString());
        }
    }
}
