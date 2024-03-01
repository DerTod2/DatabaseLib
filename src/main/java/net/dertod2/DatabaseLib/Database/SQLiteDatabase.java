package net.dertod2.DatabaseLib.Database;

import java.io.File;
import java.sql.*;
import java.util.logging.Level;

public class SQLiteDatabase extends DriverDatabase {
    private SQLiteConnection connection; // SQLite only supports one connection

    public SQLiteDatabase(File database) {
        super(null, null, database.getAbsolutePath(), null, null);
    }

    public DatabaseType getType() {
        return DatabaseType.SQLite;
    }

    public Connection getConnection() {
        try {
            if (this.connection != null && !this.connection.isClosed()) return this.connection;
            this.connection = new SQLiteConnection(DriverManager.getConnection(this.getConnectionString()));

            return this.connection;
        } catch (SQLException exc) {
            this.logger.log(Level.SEVERE, "SQLiteDatabase getConnection", exc);
            return null;
        }
    }

    protected String getConnectionString() {
        return "jdbc:sqlite:" + this.database;
    }

    public boolean tableExist(String tableName) {
        Connection connection = this.getConnection();

        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet resultSet = databaseMetaData.getTables(null, null, tableName, null);
            return resultSet.next();
        } catch (Exception exc) {
            this.logger.log(Level.WARNING, "SQLiteDatabase tableExist", exc);
            return true;
        }
    }

}