package com.publisher;

import com.models.ConnectionInfo;
import com.publisher.models.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


public class ChangeDataCapture extends Thread {
    private static final Logger logger = LogManager.getLogger(ChangeDataCapture.class);

    private ConcurrentLinkedQueue<String> queue;

    private final String database; // PostgreSQL database
    private boolean isSimpleEvent = true;
    private boolean withBeginCommit = false;

    private final ConnectionInfo connectionInfo;
    private final String publicationName;
    private final String slot;
    private final boolean slotDropIfExists;

    public ChangeDataCapture(ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> concurrentHashMap,
                             ConnectionInfo connectionInfo,
                             String publicationName,
                             String slot,
                             boolean slotDropIfExists,
                             boolean isSimpleEvent,
                             boolean withBeginCommit) throws ClassNotFoundException, SQLException {
        this.database = connectionInfo.getDatabase();
        this.isSimpleEvent = isSimpleEvent;
        this.withBeginCommit = withBeginCommit;

        if (concurrentHashMap.containsKey(database) == false) {
            this.queue = new ConcurrentLinkedQueue<>();
            concurrentHashMap.put(database, this.queue);
        }

        this.connectionInfo = connectionInfo;
        this.publicationName = publicationName;
        this.slot = slot;
        this.slotDropIfExists = slotDropIfExists;

        logger.info("the new queue created corresponding to the database: {}", database);
    }

    public void run() {
        try {
            String outputFormat = "application/json";
            Long startLSN = null;

            try (Replication replication = new Replication(connectionInfo, publicationName, slot)) {
                replication.initializeReplication(slotDropIfExists);

                while (Thread.interrupted() == false) {
                    Event eventChanges = replication.readEvent(this.isSimpleEvent, this.withBeginCommit, outputFormat, startLSN);
                    if (eventChanges != null) {
                        LinkedList<String> changes = eventChanges.getData();
                        changes.forEach(change -> this.queue.add(change));
                    } else {
                        Thread.sleep(10);
                    }
                }
            }

        } catch (InterruptedException iex) {
            logger.info("run() - listening of the database: {} has been interrupted...", this.database);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("run() - {}", ex.toString());
        }
    }
}
