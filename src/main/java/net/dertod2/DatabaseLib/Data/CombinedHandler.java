package net.dertod2.DatabaseLib.Data;

import net.dertod2.DatabaseLib.Database.DriverDatabase;
import net.dertod2.DatabaseLib.Exceptions.NoTableColumnException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The Combined handler for MySQL and SQLite
 */
public abstract class CombinedHandler extends DriverHandler {

    /**
     * Constructor for the handler
     *
     * @param driverDatabase The implementation Database
     */
    public CombinedHandler(DriverDatabase driverDatabase) {
        super(driverDatabase);
    }

    public void insert(Row tableRow) throws IllegalArgumentException, IllegalAccessException, SQLException, IOException {
        Connection connection = abstractDatabase.getConnection();
        this.insert(tableRow, connection);
        this.closeConnection(connection, null, null);
    }

    public <T extends Row> void insert(List<T> entries) throws IllegalArgumentException, IllegalAccessException, SQLException, IOException {
        if (entries.isEmpty()) return;

        Connection connection = abstractDatabase.getConnection();
        for (T tableRow : entries) this.insert(tableRow, connection);
        this.closeConnection(connection, null, null);
    }

    protected void insert(Row tableRow, Connection connection) throws IllegalArgumentException, IllegalAccessException, SQLException, IOException {
        TableCache tableCache = TableCache.getCache(tableRow.getClass(), this);

        PreparedStatement preparedStatement;
        ResultSet resultSet = null;

        Map<Column, Object> dataList = tableRow.getColumns();
        Iterator<Column> iterator = dataList.keySet().iterator();

        StringBuilder columnList = new StringBuilder();
        StringBuilder valueList = new StringBuilder();

        while (iterator.hasNext()) {
            Column column = iterator.next();
            if (column.autoIncrement() || column.columnType() == Column.ColumnType.Primary)
                continue; // Skip auto Increment - the database sets the value itself

            columnList.append("`").append(tableCache.getName(column)).append("`, ");
            valueList.append("?, ");
        }

        // Remove the leading letter
        if (!columnList.isEmpty()) columnList.delete(columnList.length() - 2, columnList.length());
        if (!valueList.isEmpty()) valueList.delete(valueList.length() - 2, valueList.length());

        preparedStatement = connection.prepareStatement("INSERT INTO `" + tableCache.getTable() + "` (" + columnList + ") VALUES (" + valueList + ");", PreparedStatement.RETURN_GENERATED_KEYS);

        iterator = dataList.keySet().iterator(); // Need again... cause iterator can't start over again

        int index = 1;
        while (iterator.hasNext()) {
            Column column = iterator.next();
            if (column.autoIncrement() || column.columnType() == Column.ColumnType.Primary)
                continue; // Skip auto Increment - the database sets the value itself
            Object columnValue = dataList.get(column);

            this.set(index++, preparedStatement, columnValue, tableCache.getType(column));
        }

        preparedStatement.executeUpdate();

        boolean hasPrimaryKey = tableCache.hasPrimaryKey();
        Column primaryKey = tableCache.getPrimaryKey();

        if (hasPrimaryKey) {
            resultSet = preparedStatement.getGeneratedKeys();
            if (resultSet.next()) tableRow.setColumn(primaryKey, resultSet.getInt(1));
        }

        tableRow.isLoaded = true;
        this.closeConnection(null, preparedStatement, resultSet);
    }

    public <T extends Row> boolean remove(Class<T> row, Helper helper) throws SQLException, IOException {
        if (helper == null) helper = new Helper();

        Connection connection = abstractDatabase.getConnection();
        PreparedStatement preparedStatement;

        TableCache tableCache = TableCache.getCache(row, this);
        String where = helper.buildWhereQueue(this.abstractDatabase.getType());

        if (!where.isEmpty()) {
            preparedStatement = connection.prepareStatement("DELETE FROM `" + tableCache.getTable() + "`" + where + ";");
            this.fillWhereQueue(tableCache, helper, preparedStatement);
        } else {
            preparedStatement = connection.prepareStatement("TRUNCATE TABLE `" + tableCache.getTable() + "`;");
        }

        preparedStatement.executeUpdate();
        this.closeConnection(connection, preparedStatement, null);

        return true;
    }

    public boolean update(Row row, Helper helper, List<String> rows) throws SQLException, IllegalArgumentException, IllegalAccessException, IOException {
        if (!row.isLoaded) return false;

        if (helper == null) helper = new Helper();

        Connection connection = abstractDatabase.getConnection();
        boolean returnResult;

        TableCache tableCache = TableCache.getCache(row.getClass(), this);
        Map<Column, Object> columnList = row.getColumns();

        StringBuilder set = new StringBuilder();

        for (Column column : columnList.keySet()) {
            if (column.columnType() == Column.ColumnType.Primary || column.autoIncrement()) continue;
            if (rows != null && !rows.isEmpty() && !rows.contains(tableCache.getName(column))) continue;

            if (!set.isEmpty()) {
                set.append(", `").append(tableCache.getName(column)).append("` = ?");
            } else {
                set.append("`").append(tableCache.getName(column)).append("` = ?");
            }
        }

        int index = 1;

        String where = helper.buildWhereQueue(this.abstractDatabase.getType());
        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE `" + tableCache.getTable() + "` SET " + set + where + ";");

        for (Column column : columnList.keySet()) {
            if (column.columnType() == Column.ColumnType.Primary || column.autoIncrement()) continue;
            if (rows != null && !rows.isEmpty() && !rows.contains(tableCache.getName(column))) continue;

            Object columnValue = columnList.get(column);
            this.set(index++, preparedStatement, columnValue, tableCache.getType(column));
        }

        this.fillWhereQueue(index, tableCache, helper, preparedStatement);

        returnResult = preparedStatement.executeUpdate() > 0;
        this.closeConnection(connection, preparedStatement, null);

        return returnResult;
    }

    public boolean update(Row row, Helper helper, Map<String, Object> content) throws SQLException, IllegalArgumentException, IOException {
        if (content == null || content.isEmpty())
            throw new NullPointerException("The specificRows argument can't be null");
        if (helper == null) helper = new Helper();

        Connection connection = abstractDatabase.getConnection();
        boolean returnResult;

        TableCache tableCache = TableCache.getCache(row.getClass(), this);
        StringBuilder set = new StringBuilder();

        for (String columnName : content.keySet()) {
            Column column = tableCache.getColumn(columnName);
            if (column == null) throw new NoTableColumnException(columnName, tableCache);

            if (column.columnType() == Column.ColumnType.Primary || column.autoIncrement()) continue;

            if (!set.isEmpty()) {
                set.append(",  `").append(tableCache.getName(column)).append("` = ?");
            } else {
                set.append("`").append(tableCache.getName(column)).append("` = ?");
            }
        }

        int index = 1;

        String where = helper.buildWhereQueue(this.abstractDatabase.getType());
        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE `" + tableCache.getTable() + "` SET " + set + where + ";");

        for (String columnName : content.keySet()) {
            Column column = tableCache.getColumn(columnName);

            if (column.columnType() == Column.ColumnType.Primary || column.autoIncrement()) continue;
            this.set(index++, preparedStatement, content.get(columnName), tableCache.getType(column));
        }

        this.fillWhereQueue(index, tableCache, helper, preparedStatement);

        returnResult = preparedStatement.executeUpdate() > 0;
        this.closeConnection(connection, preparedStatement, null);

        return returnResult;
    }

    public <T extends Row> List<T> load(Class<T> row, Helper helper) throws SQLException, IllegalArgumentException, IllegalAccessException, InstantiationException, IOException {
        if (helper == null) helper = new Helper();

        TableCache tableCache = TableCache.getCache(row, this);
        Connection connection = abstractDatabase.getConnection();

        StringBuilder get = new StringBuilder();
        StringBuilder last = new StringBuilder();

        List<Column> tableLayout = tableCache.getLayout();
        for (Column value : tableLayout) {
            if (!get.isEmpty()) get.append(", ");
            get.append("`").append(tableCache.getName(value)).append("`");
        }

        if (!helper.groupBy.isEmpty()) {
            last.append(" GROUP BY ");

            for (String field : helper.groupBy) {
                last.append("`").append(field).append("`, ");
            }

            last.delete(last.length() - 2, last.length());
        }

        if (!helper.columnSorter.isEmpty()) {
            last.append(" ORDER BY ");

            for (String field : helper.columnSorter.keySet()) {
                last.append("`").append(field).append("` ").append(helper.columnSorter.get(field)).append(", ");
            }

            last.delete(last.length() - 2, last.length());
        }

        if (helper.limit > 0) last.append(" LIMIT ").append(helper.limit);
        if (helper.offset > 0) last.append(" OFFSET ").append(helper.offset);

        String where = helper.buildWhereQueue(this.abstractDatabase.getType());
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + get + " FROM `" + tableCache.getTable() + "`" + where + last + ";");
        this.fillWhereQueue(tableCache, helper, preparedStatement);

        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet == null) return new ArrayList<>();
        List<T> results = new ArrayList<>();

        while (resultSet.next()) {
            @SuppressWarnings("deprecation")
            T newInstance = row.newInstance();

            for (Column column : tableLayout) {
                newInstance.setColumn(column, this.get(resultSet, column, tableCache.getType(column)));
            }

            newInstance.isLoaded = true;
            results.add(newInstance);
        }

        this.closeConnection(connection, preparedStatement, resultSet);

        return results;
    }

    public <T extends Row> long count(Class<T> row, Helper helper) throws SQLException, IllegalArgumentException, IOException {
        Connection connection = abstractDatabase.getConnection();
        PreparedStatement preparedStatement;
        ResultSet resultSet;
        long result = 0;

        TableCache tableCache = TableCache.getCache(row, this);

        String where = helper.buildWhereQueue(this.abstractDatabase.getType());
        preparedStatement = connection.prepareStatement("SELECT COUNT(*) AS elements FROM `" + tableCache.getTable() + "`" + where + ";");

        this.fillWhereQueue(tableCache, helper, preparedStatement);

        resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            result = resultSet.getLong("elements");
        }

        this.closeConnection(connection, preparedStatement, resultSet);

        return result;
    }

    void addColumn(TableCache tableCache, Column column) throws SQLException {
        Connection connection = abstractDatabase.getConnection();

        List<Column> layout = tableCache.getLayout();
        String whereToAdd = column.order() == -1 ? "" :
                column.order() == 1 && layout.size() > 1 ? " BEFORE `" + tableCache.getName(layout.get(column.order() + 1)) + "`" :
                        column.order() > 1 && layout.size() >= column.order() ? " AFTER `" + tableCache.getName(layout.get(column.order() - 1)) + "`" : "";

        PreparedStatement preparedStatement = connection.prepareStatement("ALTER TABLE `" + tableCache.getTable() + "` ADD `" + tableCache.getName(column) + "` " + toDatabaseType(tableCache.getType(column)) + (column.columnType() == Column.ColumnType.Unique ? " UNIQUE" : "") + whereToAdd + ";");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

    void delColumn(TableCache tableCache, String columnName) throws SQLException {
        Connection connection = abstractDatabase.getConnection();

        PreparedStatement preparedStatement = connection.prepareStatement("ALTER TABLE `" + tableCache.getTable() + "` DROP `" + columnName + "`;");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

}
