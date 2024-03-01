package net.dertod2.DatabaseLib.Database;

import net.dertod2.DatabaseLib.Data.Handler;
import net.dertod2.DatabaseLib.Data.MySQLHandler;
import net.dertod2.DatabaseLib.Data.PostGREHandler;
import net.dertod2.DatabaseLib.Data.SQLiteHandler;
import net.dertod2.DatabaseLib.Data.Types.AbstractType;
import net.dertod2.DatabaseLib.Data.Types.ListType;
import net.dertod2.DatabaseLib.Data.Types.MapType;
import net.dertod2.DatabaseLib.Data.Types.UniqueIdType;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Represents the abstract database object that all implementations use.
 */
public abstract class AbstractDatabase {
    /**
     * The logging implementation
     */
    public final Logger logger;

    /**
     * The hostname or ip address of the database
     */
    protected final String host;
    /**
     * The port where the database server can be reached
     */
    protected final Integer port;

    /**
     * The name of the database or database file/folder
     */
    protected final String database;
    /**
     * The username for login to the database
     */
    protected final String username;
    /**
     * The password for login to the database
     */
    protected final String password;
    private final Map<String, AbstractType> types;
    /**
     * The handler for all database operations
     */
    protected Handler handler;

    /**
     * Constructor
     *
     * @param host     The hostname
     * @param port     The port
     * @param database The database name
     * @param username The username
     * @param password The password
     */
    public AbstractDatabase(String host, Integer port, String database, String username, String password) {
        this.logger = Logger.getLogger("DatabaseLib");

        this.host = host;
        this.port = port;

        this.database = database;
        this.username = username;
        this.password = password;

        this.types = new HashMap<>();
        this.addDataType(new UniqueIdType(), true);
        this.addDataType(new ListType(), true); // To support one-dim lists
        this.addDataType(new MapType(), true); // To support one-dim maps
    }

    /**
     * The databaseType this implementation used
     *
     * @return DatabaseType
     */
    public abstract DatabaseType getType();

    /**
     * The name of the database used by this Handler
     *
     * @return String
     */
    public String getDatabaseName() {
        return this.database;
    }

    /**
     * Returns the dataType of the given classPath
     *
     * @param classPath The classPath String
     * @return AbstractType
     */
    public AbstractType getDataType(String classPath) {
        if (classPath.contains("<")) return this.types.get(classPath.substring(0, classPath.indexOf("<")));
        return this.types.get(classPath);
    }

    private void addDataType(AbstractType abstractType, boolean silent) {
        abstractType.database(this);

        for (String classPath : abstractType.getClassPath()) {
            this.types.put(classPath, abstractType);
        }

        if (!silent)
            this.logger.info(String.format("Injected custom type '%1$s' into '%2$s' database lib.", Arrays.toString(abstractType.getClassPath()), this.getType().name()));
    }

    /**
     * Allows adding custom data types to the handler
     *
     * @param abstractType The new datatype
     */
    public void addDataType(AbstractType abstractType) {
        this.addDataType(abstractType, false);
    }

    /**
     * Fetches a Connection out of the Connection Pool or fetches the SQLite Connection when used
     * Returns null when YAML is used
     *
     * @return connection
     */
    public Connection getConnection() {
        return null;
    }

    /**
     * Returns the {@link Handler} for working with the database
     *
     * @return Handler
     */
    public Handler getHandler() {
        if (this.handler == null) {
            switch (this.getType()) {
                case MySQL -> this.handler = new MySQLHandler((MySQLDatabase) this);
                case PostGRE -> this.handler = new PostGREHandler((PostGREDatabase) this);
                case SQLite -> this.handler = new SQLiteHandler((SQLiteDatabase) this);
                default -> {
                }
            }
        }

        return this.handler;
    }

    /**
     * The connection string to connect to the database
     *
     * @return String
     */
    protected abstract String getConnectionString();

    /**
     * Checks wherever a specific table exists in the database
     *
     * @param tableName The name of the table to search
     * @return boolean
     */
    public abstract boolean tableExist(String tableName);

    /**
     * This will return all tables inside the database
     * When a prefix was set only the tables with starts with the prefix will be returned
     * Otherwise all tables inside the database will be returned
     *
     * @return List of all tables
     */
    public abstract List<String> getAllTables();

}