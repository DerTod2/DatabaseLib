package net.dertod2.DatabaseLib.Data.Types;

import net.dertod2.DatabaseLib.Database.AbstractDatabase;

import java.lang.reflect.Type;

/**
 * The abstract implementation for custom data types
 */
public abstract class AbstractType {
    /**
     * The classPath to the custom type
     */
    private final String[] classPath;
    /**
     * The database object based on the type of database
     */
    protected AbstractDatabase abstractDatabase;

    /**
     * Initializes a custom implementation
     *
     * @param classPath The path of the classes with getClass().getName()
     */
    public AbstractType(String... classPath) {
        this.classPath = classPath;
    }

    /**
     * The different class path names
     *
     * @return Array with all names
     */
    public String[] getClassPath() {
        return this.classPath;
    }

    public void database(AbstractDatabase abstractDatabase) {
        this.abstractDatabase = abstractDatabase;
    }

    /**
     * Should convert the object value to an object of the database type equivalent to be saved into the Table
     *
     * @param value The element so set
     * @return String
     */
    public abstract String setResult(Object value);

    /**
     * Returns the string representation of this Object to a new instance of this object. The generic type is for multidimensional implementations
     *
     * @param value        The value to convert
     * @param genericTypes Used by multidimensional implementations
     * @return Object
     */
    public abstract Object getResult(String value, Type[] genericTypes);
}