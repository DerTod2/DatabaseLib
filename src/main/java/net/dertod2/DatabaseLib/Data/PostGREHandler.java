package net.dertod2.DatabaseLib.Data;

import com.google.common.collect.ImmutableList;
import net.dertod2.DatabaseLib.Data.Column.ColumnType;
import net.dertod2.DatabaseLib.Database.Pooler.PooledConnection;
import net.dertod2.DatabaseLib.Database.PostGREDatabase;
import net.dertod2.DatabaseLib.Exceptions.NoTableColumnException;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

// Differences: auto_increment -> dataType SERIAL or BIGSERIAL
// so do this: primary keys and auto_increment ONLY as BIGSERIAL and NOT as Integer
// also: PostGRE doesn't support the ` letter

/**
 * The PostGRE implementation handler
 */
public class PostGREHandler extends DriverHandler {

    public PostGREHandler(PostGREDatabase postGREDatabase) {
        super(postGREDatabase);
    }

    public void insert(Row tableRow) throws SQLException, IllegalArgumentException, IllegalAccessException, IOException {
        Connection connection = abstractDatabase.getConnection();
        if (!this.copyInsert(ImmutableList.of(tableRow), connection)) {
            this.insert(tableRow, connection);
        }

        this.closeConnection(connection, null, null);
    }

    public <T extends Row> void insert(List<T> entries) throws SQLException, IllegalArgumentException, IllegalAccessException, IOException {
        if (entries.size() <= 0) return;

        Connection connection = abstractDatabase.getConnection();
        if (!this.copyInsert(entries, connection)) {
            for (T tableRow : entries) this.insert(tableRow, connection);
        }

        this.closeConnection(connection, null, null);
    }

    public void insert(Row tableRow, Connection connection) throws IllegalArgumentException, IllegalAccessException, SQLException, IOException {
        TableCache tableCache = TableCache.getCache(tableRow.getClass(), this);

        PreparedStatement preparedStatement;
        ResultSet resultSet = null;

        Map<Column, Object> dataList = tableRow.getColumns();
        Iterator<Column> iterator = dataList.keySet().iterator();

        StringBuilder columnList = new StringBuilder();
        StringBuilder valueList = new StringBuilder();

        while (iterator.hasNext()) {
            Column column = iterator.next();
            if (column.autoIncrement() || column.columnType() == ColumnType.Primary)
                continue; // Skip auto Increment - the database sets the value itself

            columnList.append(tableCache.getName(column)).append(", ");
            valueList.append("?, ");
        }

        // Remove the leading letter
        if (!columnList.isEmpty()) columnList.delete(columnList.length() - 2, columnList.length());
        if (!valueList.isEmpty()) valueList.delete(valueList.length() - 2, valueList.length());

        preparedStatement = connection.prepareStatement("INSERT INTO " + tableCache.getTable() + " (" + columnList + ") VALUES (" + valueList + ");", PreparedStatement.RETURN_GENERATED_KEYS);

        iterator = dataList.keySet().iterator(); // Need again... cause iterator can't start over again

        int index = 1;
        while (iterator.hasNext()) {
            Column column = iterator.next();
            if (column.autoIncrement() || column.columnType() == ColumnType.Primary)
                continue; // Skip auto Increment - the database sets the value itself
            Object columnValue = dataList.get(column);

            this.set(index++, preparedStatement, columnValue, tableCache.getType(column));
        }

        preparedStatement.executeUpdate();

        boolean hasPrimaryKey = tableCache.hasPrimaryKey();
        Column primaryKey = tableCache.getPrimaryKey();

        if (hasPrimaryKey) {
            resultSet = preparedStatement.getGeneratedKeys();
            if (resultSet.next()) {
                tableRow.setColumn(primaryKey, resultSet.getInt(tableCache.getName(primaryKey))); // Always ints
            }
        }

        tableRow.isLoaded = true;
        this.closeConnection(null, preparedStatement, resultSet);
    }

    private <T extends Row> boolean copyInsert(List<T> entries, Connection connection) {
        try {
            CopyManager copyManager = ((PGConnection) ((PooledConnection) connection).getRawConnection()).getCopyAPI();
            StringBuilder stringBuilder = new StringBuilder();
            final int batchSize = 75;

            // Prepare the Rows !warning! don't mix different table data here
            // The copyRows method only works with data WITHOUT a primary Key or "return variables"

            Map<String, List<Row>> tableEntryList = new HashMap<>();
            TableCache tableCache = TableCache.getCache(entries.get(0).getClass(), this);

            for (T tableRow : entries) {
                if (tableCache.hasPrimaryKey()) return false;

                if (!tableEntryList.containsKey(tableCache.getTable()))
                    tableEntryList.put(tableCache.getTable(), new ArrayList<>());
                tableEntryList.get(tableCache.getTable()).add(tableRow);
            }

            for (String tableName : tableEntryList.keySet()) {
                PushbackReader pushBackReader = new PushbackReader(new StringReader(""), 10000);
                List<Row> tableList = tableEntryList.get(tableName);

                for (int i = 0; i < tableList.size(); i++) {
                    Row tableRow = tableList.get(i);
                    List<Column> columnList = tableCache.getLayout();

                    for (Column column : columnList) {
                        Object data = tableRow.getColumn(column);
                        Type type = tableCache.getType(column);

                        IncludedTypes primitiveWrapper = IncludedTypes.getByObject(type.getTypeName());
                        if (primitiveWrapper == IncludedTypes.String) {
                            stringBuilder.append("'").append((String) data).append("',");
                        } else if (primitiveWrapper == IncludedTypes.Unknown) {
                            stringBuilder.append("'").append(abstractDatabase.getDataType(type.getTypeName()).setResult(data)).append("',");
                        } else {
                            stringBuilder.append(data).append(",");
                        }
                    }

                    stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
                    stringBuilder.append("\n");

                    if (i % batchSize == 0) {
                        pushBackReader.unread(stringBuilder.toString().toCharArray());
                        copyManager.copyIn("COPY " + tableName + " FROM STDIN WITH CSV", pushBackReader);
                        stringBuilder.delete(0, stringBuilder.length());
                    }

                    tableRow.isLoaded = true;
                }

                pushBackReader.unread(stringBuilder.toString().toCharArray());
                copyManager.copyIn("COPY " + tableName + " FROM STDIN WITH CSV", pushBackReader);
            }

            return true;
        } catch (SQLException | IOException | IllegalArgumentException | IllegalAccessException ignored) {

        }

        return false;
    }

    public <T extends Row> boolean remove(Class<T> row, Helper helper) throws SQLException, IOException {
        if (helper == null) helper = new Helper();

        Connection connection = abstractDatabase.getConnection();
        PreparedStatement preparedStatement;

        TableCache tableCache = TableCache.getCache(row, this);
        String where = helper.buildWhereQueue(this.abstractDatabase.getType());

        if (!where.isEmpty()) {
            preparedStatement = connection.prepareStatement("DELETE FROM " + tableCache.getTable() + where + ";");
            this.fillWhereQueue(tableCache, helper, preparedStatement);
        } else {
            preparedStatement = connection.prepareStatement("TRUNCATE TABLE " + tableCache.getTable() + ";");
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
            if (column.columnType() == ColumnType.Primary) continue;
            if (rows != null && !rows.isEmpty() && !rows.contains(tableCache.getName(column))) continue;

            if (!set.isEmpty()) {
                set.append(",  ").append(tableCache.getName(column)).append(" = ?");
            } else {
                set.append(tableCache.getName(column)).append(" = ?");
            }
        }

        int index = 1;

        String where = helper.buildWhereQueue(this.abstractDatabase.getType());
        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE " + tableCache.getTable() + " SET " + set + where + ";");

        for (Column column : columnList.keySet()) {
            if (column.columnType() == ColumnType.Primary) continue;
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

            if (column.columnType() == ColumnType.Primary) continue;

            if (!set.isEmpty()) {
                set.append(",  ").append(tableCache.getName(column)).append(" = ?");
            } else {
                set.append(tableCache.getName(column)).append(" = ?");
            }
        }

        int index = 1;

        String where = helper.buildWhereQueue(this.abstractDatabase.getType());
        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE " + tableCache.getTable() + " SET " + set + where + ";");

        for (String columnName : content.keySet()) {
            Column column = tableCache.getColumn(columnName);

            if (column.columnType() == ColumnType.Primary) continue;
            this.set(index++, preparedStatement, content.get(columnName), tableCache.getType(column));
        }

        this.fillWhereQueue(index, tableCache, helper, preparedStatement);

        returnResult = preparedStatement.executeUpdate() > 0;
        this.closeConnection(connection, preparedStatement, null);

        return returnResult;
    }

    public <T extends Row> List<T> load(Class<T> row, Helper helper) throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, IOException, SecurityException {
        if (helper == null) helper = new Helper();

        TableCache tableCache = TableCache.getCache(row, this);
        Connection connection = abstractDatabase.getConnection();

        StringBuilder get = new StringBuilder();
        StringBuilder last = new StringBuilder();

        List<Column> tableLayout = tableCache.getLayout();
        for (Column value : tableLayout) {
            if (!get.isEmpty()) get.append(", ");
            get.append(tableCache.getName(value));
        }

        if (!helper.groupBy.isEmpty()) {
            last.append(" GROUP BY ");

            for (String field : helper.groupBy) {
                last.append(field).append(", ");
            }

            last.delete(last.length() - 2, last.length());
        }

        if (!helper.columnSorter.isEmpty()) {
            last.append(" ORDER BY ");

            for (String field : helper.columnSorter.keySet()) {
                last.append(field).append(" ").append(helper.columnSorter.get(field)).append(", ");
            }

            last.delete(last.length() - 2, last.length());
        }

        if (helper.limit > 0) last.append(" LIMIT ").append(helper.limit);
        if (helper.offset > 0) last.append(" OFFSET ").append(helper.offset);

        String where = helper.buildWhereQueue(this.abstractDatabase.getType());
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + get + " FROM " + tableCache.getTable() + where + last + ";");
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

    @Override
    public <T extends Row> long count(Class<T> row, Helper helper) throws SQLException, IllegalArgumentException, IOException {
        Connection connection = abstractDatabase.getConnection();
        PreparedStatement preparedStatement;
        ResultSet resultSet;
        long result = 0;

        TableCache tableCache = TableCache.getCache(row, this);

        String where = helper.buildWhereQueue(this.abstractDatabase.getType());
        preparedStatement = connection.prepareStatement("SELECT COUNT(*) AS elements FROM " + tableCache.getTable() + where + ";");

        this.fillWhereQueue(tableCache, helper, preparedStatement);

        resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            result = resultSet.getLong("elements");
        }

        this.closeConnection(connection, preparedStatement, resultSet);
        return result;
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

            preparedStatement = connection.prepareStatement("SELECT * FROM " + tableCache.getTable() + " WHERE " + tableCache.getName(column) + " = ?;");
            this.set(1, preparedStatement, primaryKey, tableCache.getType(column));

            resultSet = preparedStatement.executeQuery();
            returnResult = resultSet.next();
        } else { // Needs to check more - so a little slower
            Map<Column, Object> columnList = row.getColumns();
            Iterator<Column> iterator = columnList.keySet().iterator();

            StringBuilder stringBuilder = new StringBuilder();

            while (iterator.hasNext()) {
                Column column = iterator.next();
                if (columnList.get(column) == null) continue; // Can't check NULL variables

                if (!stringBuilder.isEmpty()) {
                    stringBuilder.append(" AND ").append(tableCache.getName(column));
                } else {
                    stringBuilder.append(tableCache.getName(column));
                }

                stringBuilder.append(" = ?");
            }

            preparedStatement = connection.prepareStatement("SELECT * FROM " + tableCache.getTable() + " WHERE " + stringBuilder + ";");
            iterator = columnList.keySet().iterator(); // New iterator because we can not start over again

            int index = 1;
            while (iterator.hasNext()) {
                Column column = iterator.next();
                Object value = columnList.get(column);
                if (value == null) continue;

                this.set(index++, preparedStatement, value, tableCache.getType(column));
            }

            resultSet = preparedStatement.executeQuery();
            returnResult = resultSet.next();
        }

        this.closeConnection(connection, preparedStatement, resultSet);
        return returnResult;
    }

    protected void createTable(TableCache tableCache) throws SQLException {
        Connection connection = abstractDatabase.getConnection();
        PreparedStatement preparedStatement;

        StringBuilder columnBuilder = new StringBuilder();
        List<Column> columnList = tableCache.getLayout();
        Iterator<Column> iterator = columnList.iterator();

        while (iterator.hasNext()) {
            Column column = iterator.next();

            columnBuilder.append(tableCache.getName(column)).append(" ");
            columnBuilder.append(column.autoIncrement() || column.columnType() == ColumnType.Primary ? "BIGSERIAL" : toDatabaseType(tableCache.getType(column)));
            if (column.columnType() != ColumnType.Normal)
                columnBuilder.append(column.columnType() == ColumnType.Primary ? " PRIMARY KEY" : " UNIQUE");

            if (iterator.hasNext()) columnBuilder.append(", ");
        }

        preparedStatement = connection.prepareStatement("CREATE TABLE " + tableCache.getTable() + " (" + columnBuilder + ");");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

    protected void addColumn(TableCache tableCache, Column column) throws SQLException {
        Connection connection = abstractDatabase.getConnection();

        PreparedStatement preparedStatement = connection.prepareStatement("ALTER TABLE " + tableCache.getTable() + " ADD " + tableCache.getName(column) + " " + toDatabaseType(tableCache.getType(column)) + (column.columnType() == ColumnType.Unique ? " UNIQUE" : "") + ";");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

    protected void delColumn(TableCache tableCache, String columnName) throws SQLException {
        Connection connection = abstractDatabase.getConnection();

        PreparedStatement preparedStatement = connection.prepareStatement("ALTER TABLE " + tableCache.getTable() + " DROP " + columnName + ";");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

    String toDatabaseType(Type type) {
        return switch (IncludedTypes.getByObject(type.getTypeName())) {
            case Boolean -> "BOOLEAN";
            case Byte, Short, Int -> "INTEGER";
            case Char, Unknown, String -> "TEXT";
            case Double, Float -> "DOUBLE PRECISION";
            case Long -> "BIGINT";
            case Timestamp -> "TIMESTAMP WITHOUT TIME ZONE";
        };
    }

}