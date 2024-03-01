package net.dertod2.DatabaseLib.Database;

/**
 * Contains all available jdbc connectors.
 */
public enum DatabaseType {
    /**
     * SQLite implementation
     */
    MySQL("com.mysql.jdbc.Driver", true, true, MySQLDatabase.class, 3306),
    /**
     * PostGRE implementation
     */
    PostGRE("org.postgresql.Driver", true, true, PostGREDatabase.class, 5432),
    /**
     * SQLite implementation
     */
    SQLite("org.sqlite.JDBC", true, false, SQLiteDatabase.class, null),
    /**
     * Disabled the database handling.
     */
    Disabled(null, false, false, null, -1);

    private final String driverPackage;
    private final Class<? extends AbstractDatabase> driverClass;
    private final Integer defaultPort;

    private final boolean usesDatabaseDriver;
    private final boolean supportConnectionPool;

    DatabaseType(String driverPackage, boolean usesDatabaseDriver, boolean supportConnectionPool, Class<? extends AbstractDatabase> driverClass, Integer defaultPort) {
        this.driverPackage = driverPackage;
        this.driverClass = driverClass;
        this.defaultPort = defaultPort;

        this.usesDatabaseDriver = usesDatabaseDriver;
        this.supportConnectionPool = supportConnectionPool;
    }

    /**
     * Gets the databaseType based on the ENUM name
     *
     * @param driverName The ENUM name
     * @return DatabaseType
     */
    public static DatabaseType byName(String driverName) {
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.name().equalsIgnoreCase(driverName)) return databaseType;
        }

        return null;
    }

    /**
     * Gets the databaseType based on the package
     *
     * @param packageName The package path
     * @return DatabaseType
     */
    public static DatabaseType byPackage(String packageName) {
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.driverPackage.equals(packageName)) return databaseType;
        }

        return null;
    }

    /**
     * Gets the databaseType based on the API Database class
     *
     * @param driverClass The internal database class
     * @return DatabaseType
     */
    public static DatabaseType byClass(Class<? extends AbstractDatabase> driverClass) {
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.driverClass.equals(driverClass)) return databaseType;
        }

        return null;
    }

    /**
     * Wherever this implementation uses an external driver
     *
     * @return boolean
     */
    public boolean isUsingDatabaseDriver() {
        return this.usesDatabaseDriver;
    }

    /**
     * Wherever this implementation can use the connection pool
     *
     * @return boolean
     */
    public boolean isUsingConnectionPool() {
        return this.supportConnectionPool;
    }

    /**
     * The package class path
     *
     * @return String
     */
    public String getDriverPackage() {
        return this.driverPackage;
    }

    /**
     * The internal implementation driver class
     *
     * @return Class extending AbstractDatabase
     */
    public Class<? extends AbstractDatabase> getDriverClass() {
        return this.driverClass;
    }

    /**
     * The default port used by the database type
     *
     * @return Integer
     */
    public Integer getDriverPort() {
        return this.defaultPort;
    }
}