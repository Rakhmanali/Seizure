package com.publisher;

import com.publisher.models.Column;
import com.publisher.models.Relation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.sql.Connection;

public class Decode {
    private static final Logger logger = LogManager.getLogger(Decode.class);

    private static final HashMap<Integer, String> dataTypes = new HashMap<Integer, String>();
    private final HashMap<Integer, Relation> relations = new HashMap<Integer, Relation>();

    public HashMap<String, Object> decodeLogicalReplicationMessage(ByteBuffer buffer, boolean withBeginCommit)
            throws ParseException, SQLException, UnsupportedEncodingException {

        HashMap<String, Object> message = new HashMap<String, Object>();

        char msgType = (char) buffer.get(0); /* (Byte1) Identifies the message as a begin message. */
        int position = 1;

        switch (msgType) {
            case 'B': /* Identifies the message as a begin message. */
                if (withBeginCommit) {
                    message.put("type", "begin");

                    // (Int64) The final LSN of the	transaction.
                    message.put("xLSNFinal", buffer.getLong(1));

                    /* (Int64) Commit timestamp of the transaction.
                     * The value is in number of microseconds since PostgreSQL epoch ( 2000 - 01 - 01 ).
                     */
                    message.put("xCommitTime", getFormattedPostgreSQLEpochDate(buffer.getLong(9)));
                    message.put("xid", buffer.getInt(17)); /* (Int32) Xid of the transaction. */
                }

                return message;

            case 'C': /* Identifies the message as a commit message. */
                if (withBeginCommit) {

                    message.put("type", "commit");

                    message.put("flags", buffer.get(1)); /* (Int8) Flags; currently unused (must be 0). */
                    message.put("commitLSN", buffer.getLong(2)); /* (Int64) The LSN of the commit. */
                    message.put("xLSNEnd", buffer.getLong(10)); /* (Int64) The end LSN of the transaction. */

                    /* (Int64) Commit timestamp of the transaction.
                     * The value is in number of microseconds since PostgreSQL epoch
                     * ( 2000 - 01 - 01 )
                     */
                    message.put("xCommitTime", getFormattedPostgreSQLEpochDate(buffer.getLong(18)));
                }

                return message;

            case 'O': /* Identifies the message as an origin message. */

                message.put("type", "origin");

                // (Int64) The LSN of the commit on the origin server.
                message.put("originLSN", buffer.getLong(1));

                buffer.position(9);
                byte[] bytes_O = new byte[buffer.remaining()];
                buffer.get(bytes_O);

                // (String) Name of the origin.
                message.put("originName", new String(bytes_O, StandardCharsets.UTF_8));

                return message;

            case 'R': /* Identifies the message as a relation message. */
                message.put("type", "relation");
                message.put("relationId", buffer.getInt(position)); /* (Int32) ID of the relation. */
                position += 4;

                buffer.position(0);
                byte[] bytes_R = new byte[buffer.capacity()];
                buffer.get(bytes_R);
                String string_R = new String(bytes_R, StandardCharsets.UTF_8);

                int firstStringEnd = string_R.indexOf(0, position); /* ASCII 0 = Null */
                int secondStringEnd = string_R.indexOf(0, firstStringEnd + 1); /* ASCII 0 = Null */

                /* (String) Namespace (empty string for pg_catalog). */
                message.put("namespaceName", string_R.substring(position, firstStringEnd));
                /* (String) Relation name. */
                message.put("relationName", string_R.substring(firstStringEnd + 1, secondStringEnd));

                /* next position = current position + string length + 1 */
                position += ((String) message.get("namespaceName")).length() + 1
                        + ((String) message.get("relationName")).length() + 1;

                buffer.position(position);

                /* (Int8) Replica identity setting for the relation (same as relreplident in pg_class). */
                message.put("relReplIdent", "" + (char) buffer.get(position));
                position += 1;

                message.put("numColumns", buffer.getShort(position)); /* (Int16) Number of columns. */
                position += 2;

                ArrayList<HashMap<String, Object>> columns = new ArrayList<HashMap<String, Object>>();

                for (int i = 0; i < ((Short) message.get("numColumns")); i++) {

                    HashMap<String, Object> column = new HashMap<String, Object>();

                    /* (Int8) Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key. */
                    column.put("isKey", buffer.get(position));
                    position += 1;

                    /* (String) Name of the column. */
                    column.put("columnName", string_R.substring(position, string_R.indexOf(0, position)));
                    position += ((String) column.get("columnName")).length() + 1;

                    /* (Int32) ID of the column's data type. */
                    column.put("dataTypeColId", buffer.getInt(position));
                    position += 4;

                    /* (Int32) Type modifier of the column (atttypmod). */
                    column.put("typeSpecificData", buffer.getInt(position));
                    position += 4;

                    columns.add(column);
                }

                message.put("columns", columns);

                return message;

            case 'Y': /* Identifies the message as a type message. */

                message.put("type", "type");

                message.put("dataTypeId", buffer.getInt(position)); /* (Int32) ID of the data type. */
                position += 4;

                buffer.position(0);
                byte[] bytes_Y = new byte[buffer.capacity()];
                buffer.get(bytes_Y);
                String string_Y = new String(bytes_Y, StandardCharsets.UTF_8);

                /* (String) Namespace (empty string for pg_catalog). */
                message.put("namespaceName", string_Y.substring(position, string_Y.indexOf(0, position)));
                position += ((String) message.get("namespaceName")).length() + 1;

                /* (String) Name of the data type. */
                message.put("dataTypeName", string_Y.substring(position, string_Y.indexOf(0, position)));

                return message;

            case 'I': /* Identifies the message as an insert message. */

                message.put("type", "insert");

                /* (Int32) ID of the relation corresponding to the ID in the relation message. */
                message.put("relationId", buffer.getInt(1));
                /* (Byte1) Identifies the following TupleData message as a new tuple ('N'). */
                message.put("tupleType", "" + (char) buffer.get(5));

                /* (TupleData) TupleData message part representing the contents of new tuple. */
                message.put("tupleData", parseTupleData(buffer, 6)[0]);

                return message;

            case 'U': /* Identifies the message as an update message. */

                message.put("type", "update");

                /* (Int32) ID of the relation corresponding to the ID in the relation message. */
                message.put("relationId", buffer.getInt(position));
                position += 4;

                /* (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old
                 * tuple ('O') or as a new tuple ('N').
                 */
                message.put("tupleType1", "" + (char) buffer.get(position));
                position += 1;

                Object[] tupleData1 = parseTupleData(buffer, position); /* TupleData N, K or O */
                message.put("tupleData1", tupleData1[0]);
                position = (Integer) tupleData1[1];

                if (message.get("tupleType1") == "N") {
                    return message;
                }

                message.put("tupleType2", "" + (char) buffer.get(position));
                position += 1;

                Object[] tupleData2 = parseTupleData(buffer, position); /* TupleData N */
                message.put("tupleData2", tupleData2[0]);

                return message;

            case 'D': /* Identifies the message as a delete message. */

                message.put("type", "delete");

                /* (Int32) ID of the relation corresponding to the ID in the relation message. */
                message.put("relationId", buffer.getInt(position));
                position += 4;

                /*(Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O'). */
                message.put("tupleType", "" + (char) buffer.get(position));
                position += 1;

                message.put("tupleData", parseTupleData(buffer, position)[0]); /* TupleData */

                return message;

            default:

                message.put("type", "error");
                message.put("description", "Unknown message type \"" + msgType + "\".");
                return message;
        }
    }

    public Object[] parseTupleData(ByteBuffer buffer, int position) throws SQLException, UnsupportedEncodingException {

        HashMap<String, Object> data = new HashMap<String, Object>();
        Object[] result = {data, position};

        String values = "";

        short columns = buffer.getShort(position); /* (Int16) Number of columns. */
        position += 2; /* short = 2 bytes */

        for (int i = 0; i < columns; i++) {

            char statusValue = (char) buffer.get(position); /* (Byte1) Either
             * identifies the
             * data as NULL
             * value ('n') or
             * unchanged TOASTed
             * value ('u') or
             * text formatted
             * value ('t'). */
            position += 1; /* byte = 1 byte */

            if (i > 0)
                values += ",";

            if (statusValue == 't') {

                int lenValue = buffer.getInt(position); /* (Int32) Length of the
                 * column value. */
                position += 4; /* int = 4 bytes */

                buffer.position(position);
                byte[] bytes = new byte[lenValue];
                buffer.get(bytes);
                position += lenValue; /* String = length * bytes */

                values += new String(bytes, StandardCharsets.UTF_8); /* (ByteN) The value of the
                 * column, in text format. */

            } else { /* statusValue = 'n' (NULL value) or 'u' (unchanged TOASTED value) */

                values = (statusValue == 'n') ? values + "null" : values + "UTOAST";
            }
        }

        data.put("numColumns", columns);
        data.put("values", "(" + values + ")");

        result[0] = data;
        result[1] = position;

        return result;
    }

    public HashMap<String, Object> decodeLogicalReplicationMessageSimple(ByteBuffer buffer, boolean withBeginCommit)
            throws ParseException, SQLException, UnsupportedEncodingException {

        HashMap<String, Object> message = new HashMap<String, Object>();

        char msgType = (char) buffer.get(0); /* (Byte1) Identifies the message as a begin message. */
        int position = 1;

        switch (msgType) {
            case 'B': /* Identifies the message as a begin message. */
                if (withBeginCommit)
                    message.put("type", "begin");
                return message;
            case 'C': /* Identifies the message as a commit message. */
                if (withBeginCommit)
                    message.put("type", "commit");
                return message;
            case 'O': /* Identifies the message as an origin message. */
                message.put("type", "origin");
                return message;
            case 'R': /* Identifies the message as a relation message. */
                Relation relation = new Relation();
                relation.setId(buffer.getInt(position)); /* (Int32) ID of the relation. */
                position += 4;

                buffer.position(0);
                byte[] bytes_R = new byte[buffer.capacity()];
                buffer.get(bytes_R);
                String string_R = new String(bytes_R, StandardCharsets.UTF_8);

                int firstStringEnd = string_R.indexOf(0, position); /* ASCII 0 = Null */
                int secondStringEnd = string_R.indexOf(0, firstStringEnd + 1); /* ASCII 0 = Null */

                /* ( String ) Namespace ( empty string for pg_catalog ) */
                relation.setNamespace(string_R.substring(position, firstStringEnd));
                /* (String) Relation name. */
                relation.setName(string_R.substring(firstStringEnd + 1, secondStringEnd));

                /* next position = current position + string length + 1 */
                position += relation.getNamespace().length() + 1 + relation.getName().length() + 1;

                buffer.position(position);

                /*
                 * (Int8) Replica identity setting for the relation (same as relreplident in pg_class).
                 */
                relation.setReplicaIdentity((char) buffer.get(position));
                position += 1;

                relation.setNumColumns(buffer.getShort(position));  /* (Int16) Number of columns. */
                position += 2;

                for (int i = 0; i < relation.getNumColumns(); i++) {
                    Column column = new Column();

                    /*
                     * (Int8) Flags for the column. Currently can be either 0 for no flags or 1 which marks
                     * the column as part of the key.
                     */
                    column.setIsKey((char) buffer.get(position));
                    position += 1;

                    /* (String) Name of the column. */
                    column.setName(string_R.substring(position, string_R.indexOf(0, position)));
                    position += column.getName().length() + 1;

                    /* (Int32) ID of the column's data type. */
                    column.setDataTypeId(buffer.getInt(position));
                    position += 4;

                    column.setDataTypeName(Decode.dataTypes.get(column.getDataTypeId()));

                    /* (Int32) Type modifier of the column (atttypmod). */
                    column.setTypeModifier(buffer.getInt(position));
                    position += 4;

                    relation.putColumn(i, column);
                }

                this.relations.put(relation.getId(), relation);

                return message;

            case 'Y': /* Identifies the message as a type message. */
                message.put("type", "type");
                return message;
            case 'I': /* Identifies the message as an insert message. */
                message.put("type", "insert");

                /*
                 * (Int32) ID of the relation corresponding to the ID in the relation message.
                 */
                int relationId_I = buffer.getInt(position);
                position += 4;

                position += 1; /* (Byte1) Identifies the following TupleData message as a new tuple ('N'). */

                message.put("relationName", this.relations.get(relationId_I).getFullName());
                message.put("tupleData", parseTupleDataSimple(relationId_I, buffer, position)[0]);

                return message;

            case 'U': /* Identifies the message as an update message. */

                message.put("type", "update");

                /*
                 * (Int32) ID of the relation corresponding to the ID in the relation message.
                 */
                int relationId_U = buffer.getInt(position);
                position += 4;

                /*
                 * (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O') or as a new tuple ('N').
                 */
                char tupleType1 = (char) buffer.get(position);
                position += 1;

                Object[] tupleData1 = parseTupleDataSimple(relationId_U, buffer, position); /* TupleData N, K or O */

                if (tupleType1 == 'N') {
                    message.put("relationName", this.relations.get(relationId_U).getFullName());
                    message.put("tupleData", tupleData1[0]);
                    return message;
                }

                position = (Integer) tupleData1[1];

                /*
                 * (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O') or as a new tuple ('N').
                 */
                position += 1;

                message.put("relationName", this.relations.get(relationId_U).getFullName());
                message.put("tupleData", parseTupleDataSimple(relationId_U, buffer, position)[0]); /* TupleData N */

                return message;

            case 'D': /* Identifies the message as a delete message. */

                message.put("type", "delete");

                /*
                 * (Int32) ID of the relation corresponding to the ID in the relation message.
                 */
                int relationId_D = buffer.getInt(position);
                position += 4;

                /*
                 * (Byte1) Either identifies the following TupleData submessage as a key ('K') or as an old tuple ('O').
                 */
                position += 1;

                message.put("relationName", this.relations.get(relationId_D).getFullName());
                message.put("tupleData", parseTupleDataSimple(relationId_D, buffer, position)[0]); /* TupleData */

                return message;

            default:

                message.put("type", "error");
                message.put("description", "Unknown message type \"" + msgType + "\".");
                return message;
        }
    }

    public Object[] parseTupleDataSimple(int relationId, ByteBuffer buffer, int position) throws SQLException,
            UnsupportedEncodingException {

        HashMap<String, Object> data = new HashMap<String, Object>();
        Object[] result = {data, position};

        short columns = buffer.getShort(position); /* (Int16) Number of columns. */
        position += 2; /* short = 2 bytes */

        for (int i = 0; i < columns; i++) {

            char statusValue = (char) buffer.get(position); /*
             * (Byte1) Either identifies the data as NULL value ('n') or unchanged TOASTed value ('u') or text
             * formatted value ('t').
             */
            position += 1; /* byte = 1 byte */

            Column column = relations.get(relationId).getColumn(i);

            if (statusValue == 't') {

                int lenValue = buffer.getInt(position); /* (Int32) Length of the column value. */
                position += 4; /* int = 4 bytes */

                buffer.position(position);
                byte[] bytes = new byte[lenValue];
                buffer.get(bytes);
                position += lenValue; /* String = length bytes */


//                if (column.getDataTypeName().startsWith("int")) { /*
//                 * (ByteN) The value of the column, in text format. Numeric types are not quoted. */
//                    data.put(column.getName(), Long.parseLong(new String(bytes, "UTF-8")));
//                } else {
//                    /* (ByteN) The value of the column, in text format. */
//                    data.put(column.getName(), new String(bytes, "UTF-8"));
//                }
                this.parseBytes(column, data, bytes);


            } else { /* statusValue = 'n' (NULL value) or 'u' (unchanged TOASTED value) */
                if (statusValue == 'n') {
                    data.put(column.getName(), null);
                } else {
                    data.put(column.getName(), "UTOAST");
                }
            }
        }

        result[0] = data;
        result[1] = position;

        return result;
    }

    private void parseBytes(Column column, HashMap<String, Object> data, byte[] bytes) throws
            UnsupportedEncodingException {

//        logger.info(String.format("data type id: %d, name: %s", column.getDataTypeId(), column.getDataTypeName()));
//        logger.info(String.format("column name: %s", column.getName()));

        switch (column.getDataTypeId()) {
//            case 16: // bool
//                boolean b = (new String(bytes, "UTF-8")).equals("t");
//                data.put(column.getName(), b);
//                break;
            case 20: // int8
            case 21: // int2
            case 23: // int4
                data.put(column.getName(), Long.parseLong(new String(bytes, StandardCharsets.UTF_8)));
                break;
//            case 700: // float4
//                data.put(column.getName(), Double.parseDouble(new String(bytes, "UTF-8")));
//                break;

            default:
                data.put(column.getName(), new String(bytes, StandardCharsets.UTF_8));
                break;
        }
    }

    public String getFormattedPostgreSQLEpochDate(long microseconds) throws ParseException {

        Date pgEpochDate = new SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01");
        Calendar cal = Calendar.getInstance();
        cal.setTime(pgEpochDate);
        cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + (int) (microseconds / 1000000));

        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z Z").format(cal.getTime());
    }

    public void loadDataTypes(Connection sqlConnection) throws SQLException {
        try (Statement statement = sqlConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            try (ResultSet resultSet = statement.executeQuery("SELECT oid, typname FROM pg_catalog.pg_type")) {
                Integer typeId;
                String typeName;
                while (resultSet.next()) {
                    typeId = resultSet.getInt(1);
                    typeName = resultSet.getString(2);
                    Decode.dataTypes.put(typeId, typeName);
                }
            }
        }
    }

    public void printBuffer(ByteBuffer buffer) throws UnsupportedEncodingException {

        byte[] bytesX = new byte[buffer.capacity()];
        buffer.get(bytesX);

        String stringX = new String(bytesX, StandardCharsets.UTF_8);

        char[] charsX = stringX.toCharArray();

        int icharX = 0;

        for (char c : charsX) {
            int ascii = c;
            System.out.println("[" + icharX + "] \t" + c + "\t ASCII " + ascii);
            icharX++;
        }
    }
}
