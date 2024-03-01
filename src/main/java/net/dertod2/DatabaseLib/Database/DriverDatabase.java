package net.dertod2.DatabaseLib.Database;

import net.dertod2.DatabaseLib.Data.DriverHandler;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public abstract class DriverDatabase extends AbstractDatabase {

    public DriverDatabase(String host, Integer port, String database, String username, String password) {
        super(host, port, database, username, password);

        try {
            Class.forName(this.getType().getDriverPackage());
            this.logger.info(String.format("Database Driver '%1$s' successfully loaded.", this.getType().name()));
        } catch (ClassNotFoundException exc) {
            this.logger.log(Level.SEVERE, "DriverDatabase initialization", exc);
        }
    }

    public List<String> getColumns(String tableName) throws SQLException {
        Connection connection = this.getConnection();

        ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, null);
        List<String> columnList = new ArrayList<>();

        while (resultSet.next()) columnList.add(resultSet.getString("COLUMN_NAME"));

        ((DriverHandler) this.getHandler()).closeConnection(connection, null, resultSet);
        return columnList;
    }

    public boolean tableExist(String tableName) {
        try {
            Connection connection = this.getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet resultSet = databaseMetaData.getTables(null, null, tableName, null);
            boolean result = resultSet.next();

            ((DriverHandler) this.getHandler()).closeConnection(connection, null, resultSet);
            return result;
        } catch (Exception exc) {
            this.logger.log(Level.SEVERE, "DriverDatabase tableExist", exc);
            return true;
        }
    }

    @Override
    public List<String> getAllTables() {
        List<String> tables = new ArrayList<>();

        try {
            Connection connection = this.getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet resultSet = databaseMetaData.getTables(null, null, "%", null);

            while (resultSet.next()) {
                tables.add(resultSet.getString("TABLE_NAME"));
            }

            ((DriverHandler) this.getHandler()).closeConnection(connection, null, resultSet);
        } catch (Exception exc) {
            this.logger.log(Level.SEVERE, "DriverDatabase getAllTables", exc);
            return null;
        }

        return tables;
    }

}