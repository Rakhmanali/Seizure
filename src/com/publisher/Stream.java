package com.publisher;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.sql.Connection;

import com.publisher.models.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Stream {
    private static final Logger logger = LogManager.getLogger(Stream.class);

    private final PGReplicationStream pgReplicationStream;
    private Long lastReceiveLSN;
    private final Decode decode;

    public static final String MIME_TYPE_OUTPUT_DEFAULT = "application/json";

    public Stream(Decode decode, Connection replicationConnection, String publication, String slot, Long lsn) throws SQLException {
        this.decode = decode;
        PGConnection pgConnection = replicationConnection.unwrap(PGConnection.class);
        if (lsn == null) {
            // More details about pgoutput options in PostgreSQL project:
            // https://github.com/postgres, source file:
            // postgres/src/backend/replication/pgoutput/pgoutput.c
            this.pgReplicationStream = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(slot)
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", publication)
                    .withStatusInterval(1, TimeUnit.SECONDS)
                    .start();

        } else {
            // Reading from LSN start position
            LogSequenceNumber startLSN = LogSequenceNumber.valueOf(lsn);

            // More details about pgoutput options in PostgreSQL project:
            // https://github.com/postgres, source file:
            // postgres/src/backend/replication/pgoutput/pgoutput.c
            this.pgReplicationStream = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(slot)
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", publication)
                    .withStatusInterval(1, TimeUnit.SECONDS)
                    .withStartPosition(startLSN)
                    .start();
        }
    }

    private final int maxMessages = 1000;

    // com.fasterxml.jackson.core
    public Event readStream(boolean isSimpleEvent, boolean withBeginCommit, String outputFormat) throws SQLException,
            InterruptedException, ParseException, UnsupportedEncodingException, JsonProcessingException {
        LinkedList<String> messages = new LinkedList<String>();
        int messagesSize = 0;
        while (messagesSize < this.maxMessages) {

            ByteBuffer buffer = this.pgReplicationStream.readPending();
            if (buffer == null) {
                break;
            }

            HashMap<String, Object> message = null;

            if (isSimpleEvent) {
                message = this.decode.decodeLogicalReplicationMessageSimple(buffer, withBeginCommit);
            } else {
                message = this.decode.decodeLogicalReplicationMessage(buffer, withBeginCommit);
            }

            if (!message.isEmpty()) { // Skip empty messages
                messages.addLast(this.convertMessage(message, outputFormat.trim().toLowerCase()));
                messagesSize++;
            }

            // Replication feedback
            this.pgReplicationStream.setAppliedLSN(this.pgReplicationStream.getLastReceiveLSN());
            this.pgReplicationStream.setFlushedLSN(this.pgReplicationStream.getLastReceiveLSN());
        }

        this.lastReceiveLSN = this.pgReplicationStream.getLastReceiveLSN().asLong();
        return new Event(messages, this.lastReceiveLSN, isSimpleEvent, withBeginCommit, false);
    }

    public String convertMessage(HashMap<String, Object> message, String outputFormat) throws JsonProcessingException {
        switch (outputFormat) {
            case MIME_TYPE_OUTPUT_DEFAULT:

                ObjectMapper mapper = new ObjectMapper();
                return mapper.writeValueAsString(message);

            default:

                throw new IllegalArgumentException("Invalid output format!");
        }
    }

    public Long getLastReceiveLSN() {
        return this.lastReceiveLSN;
    }
}
