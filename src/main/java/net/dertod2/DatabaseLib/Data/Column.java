package net.dertod2.DatabaseLib.Data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a column inside the database table
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column {

    /**
     * The name of the column inside the database table
     *
     * @return String
     */
    String name();

    /**
     * The type of the column. Either normal, unique or primary
     *
     * @return ColumnType
     */
    ColumnType columnType() default ColumnType.Normal;

    /**
     * Wherever the database should auto increment this column. Only works on primary, unique or number-based columns.
     *
     * @return boolean
     */
    boolean autoIncrement() default false;

    /**
     * Set this to -1 to ignore the order and let the database decide. Use 1 and higher to declare a sort order, never use the 0!
     *
     * @return int
     */
    int order() default -1;

    /**
     * The type of column
     */
    enum ColumnType {
        /**
         * Normal column type.
         */
        Normal,
        /**
         * Unique colum type.
         */
        Unique,
        /**
         * Primary column type.
         */
        Primary
    }
}
