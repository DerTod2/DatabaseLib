package net.dertod2.DatabaseLib.Database.Pooler;

import com.google.common.collect.ImmutableList;
import net.dertod2.DatabaseLib.Database.PooledDatabase;
import net.dertod2.DatabaseLib.Exceptions.NoPooledConnectionAvailableException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * The Connection pool handler to minimize database overload.
 */
public class ConnectionPool implements Runnable {
    private final PooledDatabase pooledDatabase;

    private final PoolSettings poolSettings;
    private final PoolStatistics poolStatistics;

    private final List<PooledConnection> availableList = new CopyOnWriteArrayList<>(); //new ArrayList<PooledConnection>();
    private final List<PooledConnection> loanedList = new CopyOnWriteArrayList<>(); // new ArrayList<PooledConnection>();
    private final Object informer = new Object();
    private volatile long lastConnectionFetched;

    /**
     * Constructor for the Connection pool
     *
     * @param pooledDatabase The implementation of the database
     * @param poolSettings   The Settings to connect to the database
     */
    public ConnectionPool(PooledDatabase pooledDatabase, PoolSettings poolSettings) {
        this.pooledDatabase = pooledDatabase;

        this.poolSettings = poolSettings;
        this.poolStatistics = new PoolStatistics();
    }

    /**
     * This tests if it is possible to connect to the database.
     *
     * @return boolean
     */
    public boolean testCredentials() {
        if (this.poolSettings.minPoolSize > this.poolSettings.maxPoolSize) return false;

        Connection connection = this.startConnection();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
            return true;
        }

        return false;
    }

    /**
     * Returns the current statistics of this pool
     *
     * @return PoolStatistics
     */
    public PoolStatistics getStatistics() {
        return this.poolStatistics;
    }

    /**
     * Returns the Settings for this pool.
     *
     * @return PoolSettings
     */
    public PoolSettings getSettings() {
        return this.poolSettings;
    }

    /**
     * Executes the Watcher Task
     **/
    public void run() {
        this.pooledDatabase.logger.finest("Working on: Thread Start");
        long startNanos = System.currentTimeMillis();

        List<PooledConnection> removableList = new ArrayList<>();

        try {
            // Work loaned Connections

            this.pooledDatabase.logger.finest("Working on: Loaned Connections");
            for (PooledConnection pooledConnection : this.loanedList) {
                try {
                    if (pooledConnection.returnToPool) { // Returns connection to the Pool
                        pooledConnection.isInPool = true;

                        pooledConnection.autoClose = true;
                        pooledConnection.currentUser = "None";

                        removableList.add(pooledConnection);
                        this.availableList.add(pooledConnection);

                        this.poolStatistics.returnedToPool++;

                        this.pooledDatabase.logger.finest("Returned Connection to the pool (fetcher executed close method)...");
                    } else if (pooledConnection.getLoanedTime() > this.poolSettings.maxLoanedTime && pooledConnection.autoClose) {
                        try {
                            pooledConnection.rawConnection.close();
                        } catch (SQLException ignored) {
                        }
                        removableList.add(pooledConnection);
                        this.poolStatistics.maxLoanedTimeReached++;

                        this.pooledDatabase.logger.finest("Force closed pooled connection 'cause of maxLoanedTime reached...");
                    }
                } catch (Exception exc) {
                    this.pooledDatabase.logger.log(Level.SEVERE, "Connection Pool Worker: Loaned", exc);
                }
            }

            this.pooledDatabase.logger.finest("Working on: Removable Connections");
            for (PooledConnection pooledConnection : removableList) {
                this.loanedList.remove(pooledConnection);
            }

            removableList.clear();

            // Work available Connections
            this.pooledDatabase.logger.finest("Working on: Available Connections");
            for (PooledConnection pooledConnection : this.availableList) {
                try {
                    if (pooledConnection.getLifetime() >= this.poolSettings.maxLifeTime) {
                        removableList.add(pooledConnection);
                        this.poolStatistics.maxLifeTimeReached++;

                        this.pooledDatabase.logger.finest("Closed pooled connection 'cause of maxLifetime reached...");
                    } else if (pooledConnection.getIdleTime() >= this.poolSettings.maxIdleTime) {
                        removableList.add(pooledConnection);
                        this.poolStatistics.maxIdleTimeReached++;

                        this.pooledDatabase.logger.finest("Closed pooled connection 'cause of maxIdleTime reached...");
                    } else if (!pooledConnection.rawConnection.isValid(1)) {
                        removableList.add(pooledConnection);
                        this.poolStatistics.invalidConnection++;

                        this.pooledDatabase.logger.finest("Closed pooled connection 'cause of invalid raw connection...");
                    }
                } catch (Exception ignored) {
                }
            }

            this.pooledDatabase.logger.finest("Working on: Invalid Connections");
            for (PooledConnection pooledConnection : removableList) {
                try {
                    pooledConnection.rawConnection.close();
                } catch (SQLException ignored) {
                }
                this.availableList.remove(pooledConnection); // Remove from pool
            }

            removableList.clear();

            // Check Pool Size
            this.pooledDatabase.logger.finest("Working on: Pool Size");
            if (this.getLastFetchTime() < this.poolSettings.startSleepMode) {
                while (this.availableList.size() < this.poolSettings.minPoolSize) {
                    if ((this.availableList.size() + this.loanedList.size()) >= this.poolSettings.maxPoolSize) {
                        this.poolStatistics.maxPoolSizeReached++;
                        break;
                    }

                    Connection connection = this.startConnection();
                    if (connection != null) {
                        this.poolStatistics.openedConnections++;
                        this.availableList.add(new PooledConnection(this, connection));

                        this.pooledDatabase.logger.finest("Opened new pooled connection cause not enough available connections");
                    }
                }
            }

            // Set Force Watch to false, in case it was set to true
            synchronized (this.informer) {
                this.informer.notifyAll();
            }

            // Wait before check again
            this.pooledDatabase.logger.finest("Working on: Statistics");
            this.poolStatistics.lastWatcherDuration = System.currentTimeMillis() - startNanos;
            this.poolStatistics.watcherRuns++;
        } catch (Exception exc) {
            this.pooledDatabase.logger.log(Level.WARNING, "Connection Pool Worker: Statistics", exc);
        }
    }

    private Connection startConnection() {
        try {
            return DriverManager.getConnection(this.poolSettings.jdbcUrl, this.poolSettings.username, this.poolSettings.password);
        } catch (SQLException exc) {
            this.pooledDatabase.logger.log(Level.SEVERE, "Connection Pool Worker: getConnection", exc);
            return null;
        }
    }

    /**
     * Returns a Connection out of the Pool
     * Warning: When no connection is available but the Pool can open more connections, this will end in a Thread lock
     * When no connections are available, this will throw an Exception.
     *
     * @return A new Connection out of the connection pool
     */
    public Connection getConnection() {
        this.lastConnectionFetched = System.currentTimeMillis();

        if (this.availableList.isEmpty()) {
            int openedConnections = this.loanedList.size();

            if (openedConnections >= this.poolSettings.maxPoolSize) {
                this.poolStatistics.maxPoolSizeReachedWhileFetching++;
                throw new NoPooledConnectionAvailableException();
            } else {
                this.poolStatistics.threadLock++;
                synchronized (this.informer) {
                    try {
                        this.informer.wait(10000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        PooledConnection pooledConnection = this.availableList.removeFirst();
        pooledConnection.loaned = System.currentTimeMillis();
        pooledConnection.returnToPool = false;
        pooledConnection.isInPool = false;

        pooledConnection.currentUser = this.getFetcher(Thread.currentThread().getStackTrace());

        this.loanedList.add(pooledConnection);

        this.pooledDatabase.logger.finest("Fetched connection out of pool...");

        return pooledConnection;
    }

    private String getFetcher(StackTraceElement[] stackTrace) {
        String fetcher = "Unknown";

        for (StackTraceElement stackTraceElement : stackTrace) {
            String className = stackTraceElement.getClassName();

            if (className.contains("net.dertod2") && !className.contains("DatabaseLib")) {
                int beginIndex = className.indexOf(".", className.indexOf(".")) + 1;
                int endIndex = className.indexOf(".", beginIndex);

                fetcher = className.substring(beginIndex, endIndex);
                break;
            }
        }

        return fetcher;
    }

    /**
     * Returns the number of available unused connections.
     *
     * @return int
     */
    public int getAvailableConnections() {
        return this.availableList.size();
    }

    /**
     * Returns the number of loaned and not available connections.
     *
     * @return int
     */
    public int getLoanedConnections() {
        return this.loanedList.size();
    }

    /**
     * Returns the time when the last connection was fetched.
     *
     * @return long
     */
    public long getLastFetched() {
        return this.lastConnectionFetched;
    }

    /**
     * Returns the elapsed milliseconds since the last connection was fetched.
     *
     * @return long
     */
    public long getLastFetchTime() {
        return System.currentTimeMillis() - this.lastConnectionFetched;
    }

    /**
     * DO NOT USE THIS CONNECTION ELEMENTS FOR WORK!<br />
     * Only to get statistics over the PooledConnection Objects
     *
     * @return a list with all active pooled connection objects
     */
    public List<PooledConnection> getActiveConnections() {
        return ImmutableList.<PooledConnection>builder().addAll(this.availableList).addAll(this.loanedList).build();
    }

    /**
     * Closes this connection pool. Should not be used by an api user.
     *
     * @return Statistics element with all remaining fetched statistics.
     */
    public PoolStatistics shutdown() {
        for (PooledConnection pooledConnection : this.availableList) {
            try {
                pooledConnection.rawConnection.close();
            } catch (SQLException ignored) {
            }
        }

        for (PooledConnection pooledConnection : this.loanedList) {
            try {
                pooledConnection.rawConnection.close();
            } catch (SQLException ignored) {
            }
        }

        this.availableList.clear();
        this.loanedList.clear();

        return this.poolStatistics;
    }

}