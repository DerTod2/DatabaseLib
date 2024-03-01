package net.dertod2.DatabaseLib.Data;

import net.dertod2.DatabaseLib.Data.Column.ColumnType;
import net.dertod2.DatabaseLib.Database.SQLiteDatabase;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The SQLite implementation handler
 */
public class SQLiteHandler extends CombinedHandler {

    public SQLiteHandler(SQLiteDatabase sqLiteDatabase) {
        super(sqLiteDatabase);
    }

    public boolean exist(Row row) throws SQLException, IllegalArgumentException, IllegalAccessException, IOException {
        Connection connection = abstractDatabase.getConnection();
        PreparedStatement preparedStatement;
        ResultSet resultSet;
        boolean returnResult;

        TableCache tableCache = TableCache.getCache(row.getClass(), this);

        if (tableCache.hasPrimaryKey()) { // Simple and faster :)
            Column column = tableCache.getPrimaryKey();
            Object primaryKey = row.getColumn(column);

            preparedStatement = connection.prepareStatement("SELECT * FROM `" + tableCache.getTable() + "` WHERE " + tableCache.getName(column) + " = ?;");
            this.set(1, preparedStatement, primaryKey, tableCache.getType(column));

        } else { // Needs to check more - so a little slower
            Map<Column, Object> columnList = row.getColumns();
            Iterator<Column> iterator = columnList.keySet().iterator();

            StringBuilder stringBuilder = new StringBuilder();

            while (iterator.hasNext()) {
                Column column = iterator.next();
                if (columnList.get(column) == null) continue; // Can't check NULL variables

                if (!stringBuilder.isEmpty()) {
                    stringBuilder.append(" AND `").append(tableCache.getName(column)).append("`");
                } else {
                    stringBuilder.append("`").append(tableCache.getName(column)).append("`");
                }

                stringBuilder.append(" = ?");
            }

            preparedStatement = connection.prepareStatement("SELECT * FROM `" + tableCache.getTable() + "` WHERE " + stringBuilder + ";");
            iterator = columnList.keySet().iterator(); // New iterator because we can not start over again

            int index = 1;
            while (iterator.hasNext()) {
                Column column = iterator.next();
                Object value = columnList.get(column);
                if (value == null) continue;

                this.set(index++, preparedStatement, value, tableCache.getType(column));
            }

        }

        resultSet = preparedStatement.executeQuery();
        returnResult = resultSet.next();

        this.closeConnection(connection, preparedStatement, resultSet);
        return returnResult;
    }

    void createTable(TableCache tableCache) throws SQLException {
        Connection connection = abstractDatabase.getConnection();
        PreparedStatement preparedStatement;

        StringBuilder columnBuilder = new StringBuilder();
        List<Column> columnList = tableCache.getLayout();
        Iterator<Column> iterator = columnList.iterator();

        while (iterator.hasNext()) {
            Column column = iterator.next();

            columnBuilder.append("`").append(tableCache.getName(column)).append("` ");
            columnBuilder.append(toDatabaseType(tableCache.getType(column)));
            if (column.columnType() != ColumnType.Normal)
                columnBuilder.append(column.columnType() == ColumnType.Primary ? " PRIMARY KEY AUTOINCREMENT" : " UNIQUE");

            if (iterator.hasNext()) columnBuilder.append(", ");
        }

        preparedStatement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS `" + tableCache.getTable() + "` (" + columnBuilder + ");");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

    String toDatabaseType(Type type) {
        return switch (IncludedTypes.getByObject(type.getTypeName())) {
            case Boolean -> "BOOLEAN";
            case Byte, Short, Int, Long -> "INTEGER";
            case Char, Unknown, String -> "TEXT";
            case Double, Float -> "REAL";
            case Timestamp -> "TIMESTAMP";
        };
    }

}