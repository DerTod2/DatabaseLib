package net.dertod2.DatabaseLib.Data;

import com.google.common.collect.ImmutableList;
import net.dertod2.DatabaseLib.Database.DatabaseType;
import net.dertod2.DatabaseLib.Exceptions.FilterNotAllowedException;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Helper {
    protected final Map<String, List<Object>> between = new HashMap<>();
    protected final Map<String, Sort> columnSorter = new HashMap<>();
    protected int limit = 0;
    protected int offset = 0;
    protected final Map<String, Integer> length = new HashMap<>();
    protected final Map<String, Filter> lengthType = new HashMap<>();
    protected final Map<String, Object> filter = new HashMap<>();
    protected final Map<String, Filter> filterType = new HashMap<>();
    protected final List<String> groupBy = new ArrayList<>();

    public Helper limit(int limit) {
        this.limit = limit;
        return this;
    }

    public Helper limit(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
        return this;
    }

    public Helper offset(int offset) {
        this.offset = offset;
        return this;
    }

    public Helper sort(String field) {
        this.columnSorter.put(field, Sort.ASC);
        return this;
    }

    public Helper sort(String field, Sort sortOrder) {
        this.columnSorter.put(field, sortOrder);
        return this;
    }

    public Helper length(String field, int length) {
        return this.length(field, length, Filter.Equals);
    }

    public Helper length(String field, int length, Filter filter) {
        if (!filter.allowedLength) {
            throw new FilterNotAllowedException("length", filter);
        } else {
            this.length.put(field, length);
            this.lengthType.put(field, filter);
        }

        return this;
    }

    public Helper filter(String field, Object value) {
        return this.filter(field, value, Filter.Equals);
    }

    public Helper filter(String field, Object value, Filter filter) {
        this.filterType.put(field, filter);
        this.filter.put(field, value);

        return this;
    }

    public Helper group(String field) {
        this.groupBy.add(field);
        return this;
    }

    public Helper between(String field, Date start, Date end) {
        return this.between(field, new Timestamp(start.getTime()), new Timestamp(end.getTime()));
    }

    public Helper between(String field, Timestamp start, Timestamp end) {
        this.between.put(field, ImmutableList.builder().add(start).add(end).build());
        return this;
    }

    public Helper between(String field, Number start, Number end) {
        this.between.put(field, ImmutableList.builder().add(start).add(end).build());
        return this;
    }

    public String buildWhereQueue(DatabaseType databaseType) {
        if (this.filter.isEmpty() && this.between.isEmpty()) return "";
        StringBuilder stringBuilder = new StringBuilder();

        // Build Where Query
        boolean isPostgres = databaseType == DatabaseType.PostGRE;

        for (String columnName : this.filter.keySet()) {
            if (!stringBuilder.isEmpty()) stringBuilder.append(" AND ");

            stringBuilder.append("`").append(columnName).append("`");
            stringBuilder.append(" ").append(this.filterType.get(columnName).getFilter()).append(" ?");
        }

        for (String columnName : this.length.keySet()) {
            if (!stringBuilder.isEmpty()) stringBuilder.append(" AND ");
            stringBuilder.append("length(").append(columnName).append(")");

            stringBuilder.append(" ").append(this.lengthType.get(columnName).getFilter()).append(" ?");
        }

        for (String columnName : this.between.keySet()) {
            if (!stringBuilder.isEmpty()) stringBuilder.append(" AND ");
            stringBuilder.append("`").append(columnName).append("`");

            stringBuilder.append(" BETWEEN ? AND ?");
        }

        stringBuilder.insert(0, " WHERE ");
        if (isPostgres) {
            return stringBuilder.toString().replace("`", "");
        } else {
            return stringBuilder.toString();
        }

    }

    public enum Sort {
        DESC("DESC"),
        ASC("ASC");

        private final String sort;

        Sort(String sort) {
            this.sort = sort;
        }

        public String getSort() {
            return this.sort;
        }
    }

    public enum Filter {
        Equals("=", true),
        GreaterThan(">", true),
        LessThan("<", true),
        GreaterEquals(">=", true),
        LessEquals("<=", true),
        NotEquals("!=", true),
        Like("LIKE", false),
        ILike("ILIKE", false),
        ;

        private final String filter;
        private final boolean allowedLength;

        Filter(String filter, boolean allowedLength) {
            this.filter = filter;
            this.allowedLength = allowedLength;
        }

        public String getFilter() {
            return this.filter;
        }

        public boolean isAllowedLength() {
            return this.allowedLength;
        }
    }


}