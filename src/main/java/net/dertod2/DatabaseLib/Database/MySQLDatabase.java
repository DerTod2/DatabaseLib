package net.dertod2.DatabaseLib.Database;

import java.sql.SQLException;

public class MySQLDatabase extends PooledDatabase {

    public MySQLDatabase(String host, int port, String database, String username, String password, boolean debugMode) throws SQLException {
        super(host, port, database, username, password, debugMode);
    }

    public MySQLDatabase(String host, int port, String database, String username, String password) throws SQLException {
        super(host, port, database, username, password);
    }

    public MySQLDatabase(String host, String database, String username, String password) throws SQLException {
        super(host, DatabaseType.MySQL.getDriverPort(), database, username, password);
    }

    public DatabaseType getType() {
        return DatabaseType.MySQL;
    }

    protected String getConnectionString() {
        return "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database;
    }

}