package com.models;

public class ConnectionInfo {
    private final String server;
    private final String database;
    private final String user;
    private final String password;
    //private final String schemaName;

    public ConnectionInfo(String server, String database, String user, String password/*, String schemaName*/) {
        this.server = server;
        this.database = database;
        this.user = user;
        this.password = password;
      //  this.schemaName = schemaName;
    }

    public String getServer() {
        return server;
    }

    public String getDatabase() {
        return database;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

//    public String getSchemaName() {
//        return schemaName;
//    }
}