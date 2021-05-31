package com.configuration;

import com.models.ConnectionInfo;
import com.models.PubSubTableInfo;
//import com.publisher.models.TableInfo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Configuration {
    private final String propertyFileName = "resources/app.properties";
    private final Properties properties;

    public Configuration() throws IOException {
        this.properties = new Properties();
        this.properties.load(new FileInputStream(propertyFileName));
    }

    private ConnectionInfo loadPublisherConnectionInfo() throws IllegalArgumentException {
        String server = properties.getProperty("publisher.connectionInfo.server");
        if (server == null) {
            throw new IllegalArgumentException("the publisher server IP is missed...");
        }

        String database = properties.getProperty("publisher.connectionInfo.database");
        if (database == null) {
            throw new IllegalArgumentException("the publisher database name is missed...");
        }

        String user = properties.getProperty("publisher.connectionInfo.user");
        if (user == null) {
            throw new IllegalArgumentException("the publisher user name is missed...");
        }

        String password = properties.getProperty("publisher.connectionInfo.password");
        if (password == null) {
            throw new IllegalArgumentException("the publisher user password is missed...");
        }

//        String schemaName = properties.getProperty("publisher.connectionInfo.schemaName");
//        if (schemaName == null) {
//            throw new IllegalArgumentException("the publisher schema name is missed...");
//        }

        //return new ConnectionInfo(server, database, user, password, schemaName);
        return new ConnectionInfo(server, database, user, password);
    }

    private ConnectionInfo loadSubscriberConnectionInfo() throws IllegalArgumentException {
        String subscriberServer = properties.getProperty("subscriber.connectionInfo.server");
        if (subscriberServer == null) {
            throw new IllegalArgumentException("the subscriber server IP is missed...");
        }

        String subscriberDatabase = properties.getProperty("subscriber.connectionInfo.database");
        if (subscriberDatabase == null) {
            throw new IllegalArgumentException("the subscriber database name is missed...");
        }

        String subscriberUser = properties.getProperty("subscriber.connectionInfo.user");
        if (subscriberUser == null) {
            throw new IllegalArgumentException("the subscriber user name is missed...");
        }

        String subscriberPassword = properties.getProperty("subscriber.connectionInfo.password");
        if (subscriberPassword == null) {
            throw new IllegalArgumentException("the subscriber user password is missed...");
        }

//        String subscriberSchemaName = properties.getProperty("subscriber.connectionInfo.schemaName");
//        if (subscriberSchemaName == null) {
//            throw new IllegalArgumentException("the subscriber schema name is missed...");
//        }

        //return new ConnectionInfo(subscriberServer, subscriberDatabase, subscriberUser, subscriberPassword, subscriberSchemaName);
        return new ConnectionInfo(subscriberServer, subscriberDatabase, subscriberUser, subscriberPassword);
    }

    private ConnectionInfo publisherConnectionInfo;

    public ConnectionInfo getPublisherConnectionInfo() throws IllegalArgumentException {
        if (this.publisherConnectionInfo == null) {
            this.publisherConnectionInfo = this.loadPublisherConnectionInfo();
        }
        return this.publisherConnectionInfo;
    }

    private ConnectionInfo subscriberConnectionInfo;

    public ConnectionInfo getSubscriberConnectionInfo() throws IllegalArgumentException {
        if (this.subscriberConnectionInfo == null) {
            this.subscriberConnectionInfo = this.loadSubscriberConnectionInfo();
        }
        return subscriberConnectionInfo;
    }

    public String getPublisherSlot() throws IllegalArgumentException {
        String slot = this.properties.getProperty("publisher.slot");
        if (slot == null) {
            throw new IllegalArgumentException("the publisher slot name is missed...");
        }
        return slot;
    }

    public String getPublicationName() throws IllegalArgumentException {
        String publicationName = this.properties.getProperty("publisher.publicationName");
        if (publicationName == null) {
            throw new IllegalArgumentException("the publication name is missed...");
        }
        return publicationName;
    }

    public int getMaxTasks() throws IllegalArgumentException {
        String maxTasks = properties.getProperty("maxTasks");
        if (maxTasks == null) {
            throw new IllegalArgumentException("the parameter - maxTasks is missed...");
        }
        return Integer.parseInt(maxTasks);
    }

    public int getBatchSize() throws IllegalArgumentException {
        String batchSize = properties.getProperty("batchSize");
        if (batchSize == null) {
            throw new IllegalArgumentException("the parameter - batchSize is missed...");
        }
        return Integer.parseInt(batchSize);
    }

//    private List<TableInfo> publicationTableInfoList;
//
//    public List<TableInfo> getPublicationTableInfoList() throws IllegalArgumentException {
//        if (this.publicationTableInfoList == null) {
//            this.publicationTableInfoList = new ArrayList<>();
//            String nopts = properties.getProperty("publisher.numberOfPublicationTables");
//            if (nopts == null) {
//                throw new IllegalArgumentException("there is necessary to set the number of publication tables...");
//            }
//            int numberOfPublicationTables = Integer.parseInt(nopts);
//
//            String schemaName;
//            String schemaNamePattern;
//
//            String name;
//            String namePattern;
//
//            String uniqueIndex;
//            String uniqueIndexPattern;
//
//            String callInfoFieldName;
//            String callInfoFieldNamePattern;
//            for (int i = 1; i <= numberOfPublicationTables; i++) {
//                schemaNamePattern = String.format("publisher.publicationTable%s.schemaName", i);
//                schemaName = this.properties.getProperty(schemaNamePattern);
//                if (schemaName == null) {
//                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", schemaNamePattern));
//                }
//
//                namePattern = String.format("publisher.publicationTable%s.name", i);
//                name = this.properties.getProperty(namePattern);
//                if (name == null) {
//                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", namePattern));
//                }
//
//                uniqueIndexPattern = String.format("publisher.publicationTable%s.uniqueIndex", i);
//                uniqueIndex = this.properties.getProperty(uniqueIndexPattern);
//                if (uniqueIndex == null) {
//                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", uniqueIndexPattern));
//                }
//
//                callInfoFieldNamePattern = String.format("publisher.publicationTable%s.callInfoFieldName", i);
//                callInfoFieldName = this.properties.getProperty(callInfoFieldNamePattern);
//                if (callInfoFieldName == null) {
//                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", callInfoFieldNamePattern));
//                }
//
//                this.publicationTableInfoList.add(new TableInfo(schemaName, name, uniqueIndex, callInfoFieldName));
//            }
//        }
//        return this.publicationTableInfoList;
//    }
//
//    private List<TableInfo> subscriptionTableInfoList;
//
//    public List<TableInfo> getSubscriptionTableInfoList() throws IllegalArgumentException {
//        if (this.subscriptionTableInfoList == null) {
//            if (this.publicationTableInfoList == null) {
//                this.publicationTableInfoList = this.getPublicationTableInfoList();
//                if (this.publicationTableInfoList == null) {
//                    throw new IllegalArgumentException("the list of publication tables is empty...");
//                }
//
//                this.subscriptionTableInfoList = new ArrayList<>();
//
//                String schemaName;
//                String schemaNamePattern;
//
//                String name;
//                String namePattern;
//
//                for (int i = 1; i <= this.publicationTableInfoList.size(); i++) {
//                    schemaNamePattern = String.format("subscriber.subscriptionTable%s.schemaName", i);
//                    schemaName = this.properties.getProperty(schemaNamePattern);
//                    if (schemaName == null) {
//                        throw new IllegalArgumentException(String.format("the parameter - %s is missing...", schemaNamePattern));
//                    }
//
//                    namePattern = String.format("subscriber.subscriptionTable%s.name", i);
//                    name = this.properties.getProperty(namePattern);
//                    if (name == null) {
//                        name = this.publicationTableInfoList.get(i).getName();
//                    }
//
//                    this.publicationTableInfoList.add(new TableInfo(schemaName, name, null, null));
//                }
//            }
//        }
//        return this.subscriptionTableInfoList;
//    }

    private List<PubSubTableInfo> pubSubTableInfoList;

    public List<PubSubTableInfo> getPubSubTableInfoList() throws IllegalArgumentException {
        if (this.pubSubTableInfoList == null) {
            this.pubSubTableInfoList = new ArrayList<>();
            String nopts = properties.getProperty("publisher.numberOfPublicationTables");
            if (nopts == null) {
                throw new IllegalArgumentException("there is necessary to set the number of publication tables...");
            }
            int numberOfPublicationTables = Integer.parseInt(nopts);

            String pubSchemaName;
            String pubSchemaNamePattern;

            String pubTableName;
            String pubTableNamePattern;

            String pubUniqueIndex;
            String pubUniqueIndexPattern;

            String pubCallInfoFieldName;
            String pubCallInfoFieldNamePattern;

            String subSchemaName;
            String subSchemaNamePattern;

            String subTableName;
            String subTableNamePattern;

            for (int i = 1; i <= numberOfPublicationTables; i++) {
                pubSchemaNamePattern = String.format("publisher.publicationTable%s.schemaName", i);
                pubSchemaName = this.properties.getProperty(pubSchemaNamePattern);
                if (pubSchemaName == null) {
                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", pubSchemaNamePattern));
                }

                pubTableNamePattern = String.format("publisher.publicationTable%s.name", i);
                pubTableName = this.properties.getProperty(pubTableNamePattern);
                if (pubTableName == null) {
                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", pubTableNamePattern));
                }

                pubUniqueIndexPattern = String.format("publisher.publicationTable%s.uniqueIndex", i);
                pubUniqueIndex = this.properties.getProperty(pubUniqueIndexPattern);
                if (pubUniqueIndex == null) {
                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", pubUniqueIndexPattern));
                }

                pubCallInfoFieldNamePattern = String.format("publisher.publicationTable%s.callInfoFieldName", i);
                pubCallInfoFieldName = this.properties.getProperty(pubCallInfoFieldNamePattern);
                if (pubCallInfoFieldName == null) {
                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", pubCallInfoFieldNamePattern));
                }

                subSchemaNamePattern = String.format("subscriber.subscriptionTable%s.schemaName", i);
                subSchemaName = this.properties.getProperty(subSchemaNamePattern);
                if (subSchemaName == null) {
                    throw new IllegalArgumentException(String.format("the parameter - %s is missing...", subSchemaNamePattern));
                }

                subTableNamePattern = String.format("subscriber.subscriptionTable%s.name", i);
                subTableName = this.properties.getProperty(subTableNamePattern);
                if (subTableName == null) {
                    subTableName = pubTableName;
                }

                this.pubSubTableInfoList.add(new PubSubTableInfo(pubSchemaName, pubTableName, pubUniqueIndex, pubCallInfoFieldName, subSchemaName, subTableName));
            }
        }
        return this.pubSubTableInfoList;
    }
}
