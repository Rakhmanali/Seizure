package com.publisher;

import com.models.ConnectionInfo;
import com.models.PubSubTableInfo;
//import com.publisher.models.TableInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Publication implements AutoCloseable {
    private final static Logger logger = LogManager.getLogger(Publication.class);

    private final ConnectionManager connectionManager;
    private final String publicationName;
    //private final List<TableInfo> publicationTableInfoList;
    private final List<PubSubTableInfo> pubSubTableInfoList;

    public Publication(ConnectionInfo connectionInfo, String publicationName, List<PubSubTableInfo> pubSubTableInfoList) {
        this.connectionManager = new ConnectionManager(connectionInfo.getServer(),
                connectionInfo.getDatabase(),
                connectionInfo.getUser(),
                connectionInfo.getPassword());
        this.publicationName = publicationName;
        this.pubSubTableInfoList = pubSubTableInfoList;
    }

    private List<String> getPublicationTables(String publicationName) throws SQLException {
        String sqlRequest = "select tablename from pg_catalog.pg_publication_tables where pubname = ?;";
        PreparedStatement statement = this.connectionManager.getSQLConnection().prepareStatement(sqlRequest);
        statement.setString(1, publicationName);
        ResultSet resultSet = statement.executeQuery();

        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(resultSet.getString("tablename"));
        }
        return result;
    }

    private void dropPublication() throws SQLException {
        List<String> tables = this.getPublicationTables(this.publicationName);
        for (String table : tables) {
            if (this.pubSubTableInfoList.stream().map(ti -> ti.getPubTableName()).anyMatch(table::equals) == false) {
                logger.info("do not forget to reset the replica identity of table: {}, because it is not in the publication list more", table);
            }
        }

        logger.info("trying to drop publication: {}", this.publicationName);
        String sqlRequest = String.format("drop publication %s", this.publicationName);
        PreparedStatement statement = this.connectionManager.getSQLConnection().prepareStatement(sqlRequest);
        statement.executeUpdate();
        logger.info("done.");
    }

    private class IdentityInfo {
        private final String tableName;
        private final String identity;

        public IdentityInfo(String tableName, String identity) {
            this.tableName = tableName;
            this.identity = identity;
        }

        public String getTableName() {
            return tableName;
        }

        public String getIdentity() {
            return identity;
        }
    }

    private List<IdentityInfo> getReplicaIdentities(List<PubSubTableInfo> pubSubTableInfoList) throws SQLException {
        if (pubSubTableInfoList == null || pubSubTableInfoList.size() == 0) {
            return null;
        }

        String oids = String.join(",", pubSubTableInfoList.stream().map(ti -> "'" + ti.getPubFullTableName() + "'::regclass").toArray(String[]::new));
        String sqlRequest =
                "SELECT relnamespace::regnamespace::text, relname, CASE relreplident" +
                        "          WHEN 'd' THEN 'default'" +
                        "          WHEN 'n' THEN 'nothing'" +
                        "          WHEN 'f' THEN 'full'" +
                        "          WHEN 'i' THEN 'index'" +
                        "        END AS replica_identity " +
                        "FROM pg_class " + String.format("WHERE oid in (%s)", oids);
        PreparedStatement statement = this.connectionManager.getSQLConnection().prepareStatement(sqlRequest);
        ResultSet resultSet = statement.executeQuery();
        List<IdentityInfo> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(new IdentityInfo(resultSet.getString(1)+"."+resultSet.getString(2), resultSet.getString(3)));
        }
        return result;
    }

    /*
        There is an additional information about REPLICA IDENTITY in
        the following sources:

        1. Replica identity for logical replication
           https://mydbanotebook.org/post/replication-key/

        2. 30.1. Publication
           https://www.postgresql.org/docs/current/logical-replication-publication.html
     */
    private void setReplicaIdentity(List<PubSubTableInfo> pubSubTableInfoList) throws SQLException {
        logger.info("****************************************************");
        logger.info("start of setReplicaIdentity()");

        List<IdentityInfo> identityInfoList = this.getReplicaIdentities(pubSubTableInfoList);

        String replicaIdentity;
        StringBuilder sb = new StringBuilder();
        for (PubSubTableInfo pubSubTableInfo : pubSubTableInfoList) {
            Optional<IdentityInfo> identityInfoOptional = identityInfoList.stream()
                    .filter(ii -> ii.getTableName().equals(pubSubTableInfo.getPubFullTableName()) == true)
                    .findFirst();
            replicaIdentity = identityInfoOptional.get().getIdentity();
            logger.info("table name: {}, replica identity: {}", pubSubTableInfo.getPubFullTableName(), replicaIdentity);
            if (replicaIdentity != null) {
                if (replicaIdentity.equals("nothing") == true || replicaIdentity.equals("default") == true) {
                    if (pubSubTableInfo.getPubUniqueIndex().length() > 0) {
                        sb.append(String.format("alter table %s replica identity using index %s;", pubSubTableInfo.getPubFullTableName(), pubSubTableInfo.getPubUniqueIndex()));
                    } else {
                        sb.append(String.format("alter table %s replica identity full;", pubSubTableInfo.getPubFullTableName()));
                    }
                }
            }
        }
        if (sb.length() > 0) {
            String sqlRequest = sb.toString();
            logger.info("trying to set the replica identity - {}", sqlRequest);
            PreparedStatement statement = this.connectionManager.getSQLConnection().prepareStatement(sqlRequest);
            statement.executeUpdate();
            logger.info("done.");
        }
        logger.info("end of setReplicaIdentity()");
        logger.info("****************************************************");
    }

    private void createPublication() throws SQLException {
        String[] tableNames = this.pubSubTableInfoList.stream().map(ti -> ti.getPubFullTableName()).toArray(String[]::new);
        String sqlRequest = String.format("create publication %s for table ", this.publicationName) + String.join(",", tableNames) + ";";
        PreparedStatement statement = this.connectionManager.getSQLConnection().prepareStatement(sqlRequest);
        statement.executeUpdate();

        this.setReplicaIdentity(this.pubSubTableInfoList);
    }

    public void initializePublication() throws ClassNotFoundException, SQLException {

        this.connectionManager.createSQLConnection();

        String sqlRequest = "select 1 from pg_catalog.pg_publication WHERE pubname = ?";
        PreparedStatement statement = this.connectionManager.getSQLConnection().prepareStatement(sqlRequest);
        statement.setString(1, this.publicationName);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            this.dropPublication();
        }
        this.createPublication();
    }

    public void close() throws SQLException {
        this.connectionManager.closeSQLConnection();
    }
}
