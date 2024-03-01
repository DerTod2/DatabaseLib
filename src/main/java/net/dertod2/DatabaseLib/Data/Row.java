package net.dertod2.DatabaseLib.Data;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Note that this class always needs an empty constructor
 *
 * @author DerTod2
 */
public abstract class Row {
    boolean isLoaded = false;

    public Row() {
    }

    /**
     * Defines if this object was loaded out of the database or inserted into the database
     *
     * @return boolean
     */
    public boolean isLoaded() {
        return this.isLoaded;
    }

    /**
     * Only use this method when you know what you're doing
     */
    public void setLoaded() {
        this.isLoaded = true;
    }

    // #########################

    Object getColumn(Column annotation) throws IllegalArgumentException, IllegalAccessException {
        Map<Column, Field> fields = TableCache.getCache(getClass()).getColumnFields();

        for (Entry<Column, Field> entry : fields.entrySet()) {
            if (entry.getKey().equals(annotation)) return entry.getValue().get(this);
        }

        return null;
    }

    Map<Column, Object> getColumns() throws IllegalArgumentException, IllegalAccessException {
        Map<Column, Field> fields = TableCache.getCache(getClass()).getColumnFields();
        Map<Column, Object> data = new HashMap<>();

        for (Entry<Column, Field> entry : fields.entrySet()) {
            data.put(entry.getKey(), entry.getValue().get(this));
        }

        return data;
    }

    void setColumn(Column annotation, Object value) throws IllegalArgumentException, IllegalAccessException {
        Map<Column, Field> fields = TableCache.getCache(getClass()).getColumnFields();

        for (Entry<Column, Field> entry : fields.entrySet()) {
            if (entry.getKey().equals(annotation)) {
                if (entry.getValue().isEnumConstant()) {
                    entry.getValue().set(this, Enum.valueOf(entry.getValue().getType().asSubclass(Enum.class), (String) value));
                } else {
                    entry.getValue().set(this, value);
                }
                entry.getValue().set(this, value);
                return;
            }
        }

    }

}