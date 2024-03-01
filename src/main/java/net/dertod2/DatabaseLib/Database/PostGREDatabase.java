package net.dertod2.DatabaseLib.Database;

import java.sql.SQLException;

public class PostGREDatabase extends PooledDatabase {

    public PostGREDatabase(String host, int port, String database, String username, String password, boolean debugMode) throws SQLException {
        super(host, port, database, username, password, debugMode);
    }

    public PostGREDatabase(String host, int port, String database, String username, String password) throws SQLException {
        super(host, port, database, username, password);
    }

    public PostGREDatabase(String host, String database, String username, String password) throws SQLException {
        super(host, DatabaseType.PostGRE.getDriverPort(), database, username, password);
    }

    public DatabaseType getType() {
        return DatabaseType.PostGRE;
    }

    protected String getConnectionString() {
        return "jdbc:postgresql://" + this.host + ":" + this.port + "/" + this.database;
    }

}