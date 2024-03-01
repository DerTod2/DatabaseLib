package net.dertod2.DatabaseLib.Data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.dertod2.DatabaseLib.Exceptions.MultiplePrimaryKeysException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.*;

public class TableCache {
    private static final Map<String, TableCache> cache = new HashMap<>();

    private final String className;
    private final List<Column> uniques;
    private final Map<Column, Field> fields;
    private final Map<String, Column> columns;
    private final Map<Column, Type> types;
    private final Map<Column, String> names;
    private final List<Column> layout;
    private final Map<UUID, Boolean> initializes;
    private String tableName;
    private Constructor<? extends Row> constructor;
    private Column primary;

    public TableCache(Class<? extends Row> clazz) {
        this.className = clazz.getName();
        this.tableName = clazz.getAnnotation(Table.class).name();

        if (this.tableName.length() <= 0) this.tableName = clazz.getSimpleName().toLowerCase();

        try {
            this.constructor = clazz.getDeclaredConstructor();
        } catch (NoSuchMethodException exc) {
            System.err.println("The class '" + clazz.getSimpleName() + "' needs an empty constructor to allow all database operations for DatabaseHandler!");
            System.exit(1); // Forces a shutdown
        }

        List<Column> uniques = new ArrayList<>();

        Map<Column, Field> fields = new HashMap<>();
        Map<String, Column> columns = new HashMap<>();
        Map<Column, Type> types = new HashMap<>();
        Map<Column, String> names = new HashMap<>();

        List<Field> classFields = TableCache.getAllDeclaredFields(new ArrayList<>(), clazz); //clazz.getDeclaredFields();
        for (Field field : classFields) {
            Column column = field.getAnnotation(Column.class);
            if (column == null) continue;

            field.setAccessible(true);

            String name = column.name();
            if (name.length() <= 0) name = field.getName().toLowerCase();

            fields.put(column, field);
            columns.put(name, column);
            types.put(column, field.getGenericType());
            names.put(column, name);

            switch (column.columnType()) {
                case Normal:
                    break;
                case Primary:
                    if (this.primary != null) throw new MultiplePrimaryKeysException(clazz.getName());
                    this.primary = column;
                    break;
                case Unique:
                    uniques.add(column);
                    break;
            }
        }

        List<Column> layout = new ArrayList<>(fields.keySet());
        layout.sort(new ColumnClassSorter());

        this.uniques = ImmutableList.copyOf(uniques);

        this.fields = ImmutableMap.copyOf(fields);
        this.columns = ImmutableMap.copyOf(columns);
        this.types = ImmutableMap.copyOf(types);
        this.names = ImmutableMap.copyOf(names);

        this.layout = ImmutableList.copyOf(layout);

        this.initializes = new HashMap<>();
    }

    public static TableCache getCache(Class<? extends Row> clazz) {
        return TableCache.getCache(clazz, null);
    }

    public static TableCache getCache(Class<? extends Row> clazz, Handler handler) {
        if (!cache.containsKey(clazz.getName())) cache.put(clazz.getName(), new TableCache(clazz));

        TableCache tableCache = cache.get(clazz.getName());
        if (handler != null && !tableCache.initializes.containsKey(handler.handlerUniqueId)) {
            try {
                handler.updateTable(tableCache);
            } catch (SQLException exc) {
                exc.printStackTrace();
            }
            tableCache.initializes.put(handler.handlerUniqueId, true);
        }

        return tableCache;
    }

    public static List<Field> getAllDeclaredFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.asList(type.getDeclaredFields()));

        if (type.getSuperclass() != null) {
            TableCache.getAllDeclaredFields(fields, type.getSuperclass());
        }

        return fields;
    }

    /**
     * Gets the name of the table class file
     */
    public String getName() {
        return this.className;
    }

    /**
     * Gets the name of the table in the database, set over the {@link Table} annotation
     */
    public String getTable() {
        return this.tableName;
    }

    /**
     * Creates a new Instance of the TableRow class
     *
     * @return A new class instance
     */
    public <T extends Row> Row createClass() {
        try {
            return this.constructor.newInstance();
        } catch (Exception exc) {
            return null;
        }
    }

    /**
     * Checks if this table has a primary key to identify the different rows
     *
     * @return true when a primary key exists, otherwise false
     */
    public boolean hasPrimaryKey() {
        return this.primary != null;
    }

    /**
     * Checks if the table has one or more unique keys
     *
     * @return true when at least one unique key exist, otherwise false
     */
    public boolean hasUniqueKeys() {
        return !this.uniques.isEmpty();
    }

    public boolean hasColumn(String columnName) {
        return this.columns.containsKey(columnName);
    }

    public Column getPrimaryKey() {
        return this.primary;
    }

    public List<Column> getUniqueKeys() {
        return this.uniques;
    }

    public Map<Column, Field> getColumnFields() {
        return this.fields;
    }

    public Map<String, Column> getStringColumns() {
        return this.columns;
    }

    public List<Column> getLayout() {
        return this.layout;
    }

    public Type getType(Column column) {
        return this.types.get(column);
    }

    public Field getField(Column column) {
        return this.fields.get(column);
    }

    public Column getColumn(String columnName) {
        return this.columns.get(columnName);
    }

    public String getName(Column column) {
        return this.names.get(column);
    }

    public static class ColumnClassSorter implements Comparator<Column> {

        public int compare(Column o1, Column o2) {
            if (o1.order() != -1 && o2.order() != -1) {
                if (o1.order() > o2.order()) return 1;
                if (o1.order() < o2.order()) return -1;
            }

            return 0;
        }

    }

}