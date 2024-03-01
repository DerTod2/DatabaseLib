package net.dertod2.DatabaseLib.Data;

import net.dertod2.DatabaseLib.Data.Types.AbstractType;
import net.dertod2.DatabaseLib.Database.DriverDatabase;
import net.dertod2.DatabaseLib.Exceptions.NoTableColumnException;

import java.io.*;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public abstract class DriverHandler extends Handler {

    public DriverHandler(DriverDatabase driverDatabase) {
        super(driverDatabase);
    }

    void set(int index, PreparedStatement preparedStatement, Object value, Type type) throws SQLException, IOException {
        switch (IncludedTypes.getByObject(type.getTypeName())) {
            case Boolean -> preparedStatement.setBoolean(index, (Boolean) value);
            case Byte -> preparedStatement.setByte(index, (Byte) value);
            case Char -> preparedStatement.setString(index, ((Character) value).toString());
            case Double -> preparedStatement.setDouble(index, (Double) value);
            case Float -> preparedStatement.setFloat(index, (Float) value);
            case Int -> preparedStatement.setInt(index, (Integer) value);
            case Long -> preparedStatement.setLong(index, (Long) value);
            case Short -> preparedStatement.setShort(index, (Short) value);
            case String -> preparedStatement.setString(index, (String) value);
            case Timestamp -> preparedStatement.setTimestamp(index, (Timestamp) value);
            case Unknown -> {
                AbstractType abstractType = this.abstractDatabase.getDataType(type.getTypeName());
                if (abstractType != null) {
                    preparedStatement.setString(index, abstractType.setResult(value));
                } else if (value instanceof Serializable serializable) {

                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

                    objectOutputStream.writeObject(serializable);
                    objectOutputStream.flush();
                    objectOutputStream.close();

                    preparedStatement.setString(index, Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray()));
                } else {
                    this.abstractDatabase.logger.severe(String.format("Unknown DataType to handle: %1$s", value.getClass().getName()));
                }
            }
        }
    }

    Object get(ResultSet resultSet, Column column, Type type) throws SQLException {
        switch (IncludedTypes.getByObject(type.getTypeName())) {
            case Boolean -> {
                return resultSet.getBoolean(column.name());
            }
            case Byte -> {
                return resultSet.getByte(column.name());
            }
            case Char -> {
                return resultSet.getString(column.name()).charAt(0);
            }
            case Double -> {
                return resultSet.getDouble(column.name());
            }
            case Float -> {
                return resultSet.getFloat(column.name());
            }
            case Int -> {
                return resultSet.getInt(column.name());
            }
            case Long -> {
                return resultSet.getLong(column.name());
            }
            case Short -> {
                return resultSet.getShort(column.name());
            }
            case String -> {
                return resultSet.getString(column.name());
            }
            case Timestamp -> {
                return resultSet.getTimestamp(column.name());
            }
            case Unknown -> {
                AbstractType abstractType = this.abstractDatabase.getDataType(type.getTypeName());
                if (abstractType != null) {
                    return abstractType.getResult(resultSet.getString(column.name()), type instanceof ParameterizedType ? ((ParameterizedType) type).getActualTypeArguments() : null);
                } else {
                    try {
                        Class<?> targetClass = Class.forName(type.getTypeName());
                        if (targetClass.isAssignableFrom(Serializable.class)) {
                            ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(resultSet.getString(column.name()))));

                            Object object = objectInputStream.readObject();
                            objectInputStream.close();

                            return object;
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        this.abstractDatabase.logger.severe(String.format("Unknown DataType to handle: %1$s", type.getTypeName()));

        return null;
    }

    protected int fillWhereQueue(TableCache tableCache, Helper loadHelper, PreparedStatement preparedStatement) throws SQLException, IOException {
        return this.fillWhereQueue(1, tableCache, loadHelper, preparedStatement);
    }

    protected int fillWhereQueue(int index, TableCache tableCache, Helper loadHelper, PreparedStatement preparedStatement) throws SQLException, IOException {
        for (String columnName : loadHelper.filter.keySet()) {
            Column column = tableCache.getColumn(columnName);
            if (column == null) throw new NoTableColumnException(columnName, tableCache);
            Object columnValue = loadHelper.filter.get(columnName);

            this.set(index++, preparedStatement, columnValue, tableCache.getType(column));
        }

        for (String columnName : loadHelper.length.keySet()) {
            Column column = tableCache.getColumn(columnName);
            if (column == null) throw new NoTableColumnException(columnName, tableCache);
            Integer columnValue = loadHelper.length.get(columnName);

            this.set(index++, preparedStatement, columnValue, Integer.class);
        }

        for (String columnName : loadHelper.between.keySet()) {
            Column column = tableCache.getColumn(columnName);
            if (column == null) throw new NoTableColumnException(columnName, tableCache);
            List<Object> columnValue = loadHelper.between.get(columnName);

            this.set(index++, preparedStatement, columnValue.get(0), tableCache.getType(column));
            this.set(index++, preparedStatement, columnValue.get(1), tableCache.getType(column));
        }

        return index;
    }

    abstract void createTable(TableCache tableCache) throws SQLException;

    protected void updateTable(TableCache tableCache) throws SQLException {
        if (!abstractDatabase.tableExist(tableCache.getTable())) {
            this.createTable(tableCache);
            return;
        }

        List<String> removeList = new ArrayList<>();
        List<Column> addList = new ArrayList<>(tableCache.getLayout());
        List<String> existingList = ((DriverDatabase) abstractDatabase).getColumns(tableCache.getTable());

        for (String columnName : existingList) {
            if (!tableCache.hasColumn(columnName)) {
                removeList.add(columnName);
            } else {
                addList.remove(tableCache.getColumn(columnName));
            }
        }

        addList.sort(new TableCache.ColumnClassSorter()); // Re-Sort, not needed ?

        // Add and remove the columns
        for (String columnName : removeList) this.delColumn(tableCache, columnName);
        for (Column column : addList) this.addColumn(tableCache, column);
    }

    abstract void addColumn(TableCache tableCache, Column column) throws SQLException;

    abstract void delColumn(TableCache tableCache, String columnName) throws SQLException;

    abstract String toDatabaseType(Type type);

    public void closeConnection(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
        try {
            if (resultSet != null) resultSet.close();
        } catch (SQLException ignored) {
        }
        try {
            if (preparedStatement != null) preparedStatement.close();
        } catch (SQLException ignored) {
        }
        try {
            if (connection != null) connection.close();
        } catch (SQLException ignored) {
        }
    }

}