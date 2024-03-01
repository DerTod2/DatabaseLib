package net.dertod2.DatabaseLib.Exceptions;

import net.dertod2.DatabaseLib.Data.TableCache;

import java.io.Serial;

public class NoTableColumnException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = -7608612414473669807L;

    private final String columnName;
    private final TableCache tableCache;

    public NoTableColumnException(String columnName, TableCache tableCache) {
        this.columnName = columnName;
        this.tableCache = tableCache;
    }

    public String getMessage() {
        return "The Column '" + this.columnName + "' does not exist in the table '" + this.tableCache.getTable() + "' and class '" + this.tableCache.getName() + "'";
    }
}