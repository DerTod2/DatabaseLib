package net.dertod2.DatabaseLib.Data;

import com.google.common.collect.ImmutableList;
import net.dertod2.DatabaseLib.Data.Helper.Sort;
import net.dertod2.DatabaseLib.Database.AbstractDatabase;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The abstract handler class for all implementations
 */
public abstract class Handler {
    protected final AbstractDatabase abstractDatabase;
    protected final UUID handlerUniqueId;

    public Handler(AbstractDatabase abstractDatabase) {
        this.abstractDatabase = abstractDatabase;
        this.handlerUniqueId = UUID.randomUUID();
    }

    /**
     * Inserts a single row inside the database
     *
     * @param row The row to insert
     */
    public abstract void insert(Row row) throws IllegalArgumentException, IllegalAccessException, SQLException, IOException;

    /**
     * Inserts many rows at the same time into the database. Some databases support faster adding of multiple rows at once
     *
     * @param entries A list of all entries that should be added to the database
     */
    public abstract <T extends Row> void insert(List<T> entries) throws IllegalArgumentException, IllegalAccessException, SQLException, IOException;

    /**
     * Removes the given row out of the database. Works only for tables with primary keys
     *
     * @param row The element that should be removed.
     * @return Wherever the entry was removed.
     */
    public boolean remove(Row row) throws SQLException, IOException, IllegalArgumentException, IllegalAccessException {
        TableCache tableCache = TableCache.getCache(row.getClass(), this);

        if (tableCache.hasPrimaryKey()) {
            return this.remove(row.getClass(), new Helper().filter(tableCache.getName(tableCache.getPrimaryKey()), row.getColumn(tableCache.getPrimaryKey())));
        }

        return false;
    }

    /**
     * Removes all matching rows (by the filter) out of the database table given with the row
     *
     * @param row    The Table Information
     * @param helper The sorting helper
     * @return Wherever the operation was successful or not.
     */
    public abstract <T extends Row> boolean remove(Class<T> row, Helper helper) throws SQLException, IOException;

    /**
     * Updates the row in the database to match the new data out of the row argument. Works only for tables with primary keys or unique keys
     *
     * @param row The database loaded element with updated data.
     * @return Wherever the entry was updated or not.
     */
    public boolean update(Row row) throws IllegalArgumentException, IllegalAccessException, SQLException, IOException {
        if (!row.isLoaded) return false;

        TableCache tableCache = TableCache.getCache(row.getClass(), this);

        Helper helper = new Helper();
        if (tableCache.hasPrimaryKey()) {
            helper.filter(tableCache.getName(tableCache.getPrimaryKey()), row.getColumn(tableCache.getPrimaryKey()));
        } else if (tableCache.hasUniqueKeys()) {
            for (Column column : tableCache.getUniqueKeys()) {
                helper.filter(tableCache.getName(column), row.getColumn(column));
            }
        }

        return this.update(row, helper);
    }

    /**
     * Updates all matching rows inside a database table given by row and the filter
     *
     * @param row    The element with changed data.
     * @param helper The filter to select only specific table rows.
     * @return Wherever the execution was successful or not.
     */
    public boolean update(Row row, Helper helper) throws IllegalArgumentException, IllegalAccessException, SQLException, IOException {
        return this.update(row, helper, ImmutableList.of());
    }

    /**
     * Updates only the given rows inside a database table matching all rows by the filter
     *
     * @param row     The element with changed data.
     * @param helper  The filter to select only specific table rows.
     * @param columns The filter to select the updated columns
     * @return Wherever the execution was successful or not.
     */
    public abstract boolean update(Row row, Helper helper, List<String> columns) throws SQLException, IllegalArgumentException, IllegalAccessException, IOException;

    /**
     * Updates only the given columns to the given data in content that's matching the filter
     *
     * @param row     The Table Information
     * @param helper  The filter to select only specific table rows.
     * @param content The Map with column names and data
     * @return Wherever the execution was successful or not.
     */
    public abstract boolean update(Row row, Helper helper, Map<String, Object> content) throws Exception;

    /**
     * Loads the latest entry of the given table out of the database. Works only for tables with a primary key
     *
     * @param row The Table Information
     * @return The loaded element or null
     */
    public <T extends Row> T loadLast(Class<T> row) throws IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, SQLException, IOException {
        TableCache tableCache = TableCache.getCache(row, this);
        if (tableCache.hasPrimaryKey()) {
            List<T> list = this.load(row, new Helper().limit(1).sort(tableCache.getName(tableCache.getPrimaryKey()), Sort.DESC));

            return !list.isEmpty() ? list.getFirst() : null;
        }

        return null;
    }

    /**
     * Loads the first entry of the given table out of the database. Works only for tables with a primary key
     *
     * @param row The Table Information
     * @return The loaded element or null
     */
    public <T extends Row> T loadFirst(Class<T> row) throws IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, SQLException, IOException {
        TableCache tableCache = TableCache.getCache(row, this);
        if (tableCache.hasPrimaryKey()) {
            List<T> list = this.load(row, new Helper().limit(1).sort(tableCache.getName(tableCache.getPrimaryKey()), Sort.ASC));

            return !list.isEmpty() ? list.getFirst() : null;
        }

        return null;
    }

    /**
     * Loads only one element out of the database
     *
     * @param row    The Table Information
     * @param helper The filter to sort out other elements.
     * @return The loaded element or null
     */
    public <T extends Row> T loadOne(Class<T> row, Helper helper) throws IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, SQLException, IOException {
        helper.limit = 1;

        List<T> list = this.load(row, helper);
        return !list.isEmpty() ? list.getFirst() : null;
    }

    /**
     * Loads all data out of the database and the given table
     * Warning: This method tries to create a new instance over an empty constructor
     *
     * @param row The Table Information
     * @return A list with all elements of the table.
     */
    public <T extends Row> List<T> load(Class<T> row) throws SQLException, IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, IOException {
        return this.load(row, new Helper());
    }

    /**
     * Loads all matching rows by the filter out of the given database table
     * Warning: This method tries to create a new instance over an empty constructor
     *
     * @param row    The Table Information
     * @param helper The helper to order and sort out matching elements
     * @return A list with all matching elements of the table.
     */
    public abstract <T extends Row> List<T> load(Class<T> row, Helper helper) throws SQLException, IllegalArgumentException, IllegalAccessException, InstantiationException, SecurityException, IOException;

    /**
     * Checks if the given element exists inside the database.
     * Works only for tables with primary keys and/or unique keys
     *
     * @param row The element
     * @return Wherever the element exists or not
     */
    public abstract boolean exist(Row row) throws SQLException, IllegalArgumentException, IllegalAccessException, IOException;

    /**
     * Returns the number of rows inside the table that are matching the helper
     *
     * @param row    The Table Information
     * @param helper Allows sorting out only matching elements
     * @return The number of elements matching the helper
     */
    public abstract <T extends Row> long count(Class<T> row, Helper helper) throws SQLException, IllegalArgumentException, IOException;

    /**
     * Updates the table in the database to match the layout of the class
     *
     * @param tableCache The TableCache file
     */
    protected abstract void updateTable(TableCache tableCache) throws SQLException;

}