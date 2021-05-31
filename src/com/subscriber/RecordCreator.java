package com.subscriber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.models.PubSubTableInfo;
import com.subscriber.models.CallInfo;
import com.subscriber.models.GeneratedRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.List;

public class RecordCreator extends Thread {
    private static final Logger logger = LogManager.getLogger(RecordCreator.class);

    private final Connection connection;
    private final List<String> records;
    private final List<PubSubTableInfo> pubSubTableInfoList;

    public RecordCreator(Connection connection, List<String> records, List<PubSubTableInfo> pubSubTableInfoList) {
        this.connection = connection;
        this.records = records;
        this.pubSubTableInfoList = pubSubTableInfoList;

        logger.info("the incoming records count: {}", records.size());
    }

    public void run() {
        try {
            String insertStatement = this.createInsertStatements();
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(insertStatement);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("run() - {}", ex.toString());
        }
    }

    /*
        example of writing arrays

        insert into bool_test_table (array_bool_field_0)
        values ('{"t", "f", "t"}');

        insert into real_test_table (array_real_field_0)
        values ('{"12.34", "56.78", "90.12"}');

        insert into text_test_table (array_text_field_0)
        values ('{"Gulistan", "Amsterdam", "London", "Moscow"}');


     */

    public String createInsertStatements() throws Exception {
        StringBuilder rSb = new StringBuilder();
        StringBuilder fSb;
        StringBuilder vSb;

        Object value;
        ObjectMapper objectMapper = new ObjectMapper();

        for (String record : this.records) {
            fSb = new StringBuilder();
            vSb = new StringBuilder();

            GeneratedRecord generatedRecord = null;
            try {
                generatedRecord = objectMapper.readValue(record, GeneratedRecord.class);
            } catch (Exception ex) {
            }

            if (generatedRecord != null) {

                String relationName = generatedRecord.getRelationName();
                PubSubTableInfo pubSubTableInfo = this.pubSubTableInfoList
                        .stream()
                        .filter(ti -> ti.getPubFullTableName().equalsIgnoreCase(relationName) == true)
                        .findFirst()
                        .orElse(null);

                for (String fieldName : generatedRecord.getTupleData().keySet()) {

                    if (fieldName.equalsIgnoreCase(pubSubTableInfo.getPubCallInfoFieldName()) == true) {
                        value = generatedRecord.getTupleData().get(fieldName);
                        if (value == null) {
                            continue;
                        }

                        String jsonData = value.toString();
                        CallInfo callInfo = objectMapper.readValue(jsonData, CallInfo.class);

                        String tn = callInfo.getTn();
                        if (tn != null) {
                            fSb.append("tn,");
                            vSb.append(String.format("'%s',", tn));
                        }

                        String ip = callInfo.getIp();
                        if (ip != null) {
                            fSb.append("ip,");
                            vSb.append(String.format("'%s',", ip));
                        }

                        fSb.append("id,");
                        vSb.append(String.format("%d,", callInfo.getId()));

                        String fn = callInfo.getFn();
                        if (fn != null) {
                            fSb.append("fn,");
                            vSb.append(String.format("'%s',", fn));
                        }
                    } else {

                        value = generatedRecord.getTupleData().get(fieldName);
                        if (value != null) {
                            fSb.append(String.format("%s,", fieldName));

                            if (value instanceof String) {
                                vSb.append("'").append(value).append("',");
                            } else {
                                vSb.append(value).append(",");
                            }
                        }
                    }
                }


                fSb.append("action");
                vSb.append(String.format("'%s'", generatedRecord.getType()));

                rSb.append(String.format("insert into %s (%s) ", pubSubTableInfo.getSubFullTableName(), fSb));
                rSb.append(String.format("values (%s);", vSb));
            }
        }

        return rSb.toString();
    }

    public Connection getConnection() {
        return this.connection;
    }
}
