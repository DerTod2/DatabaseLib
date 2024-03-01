package net.dertod2.DatabaseLib.Database.Pooler;

public class PoolStatistics {

    protected long lastWatcherDuration;
    protected int watcherRuns = 0;
    protected int openedConnections = 0;

    protected int threadLock = 0;
    protected int maxPoolSizeReachedWhileFetching = 0;
    protected int maxPoolSizeReached = 0;
    protected int invalidConnection = 0;
    protected int maxLifeTimeReached = 0;
    protected int maxIdleTimeReached = 0;
    protected int returnedToPool = 0;
    protected int maxLoanedTimeReached = 0;

    protected PoolStatistics() {
    }

    /**
     * The Time the watcher needed to check all connections
     *
     * @return the lastWatcherDuration
     */
    public synchronized long getLastWatcherDuration() {
        return lastWatcherDuration;
    }

    /**
     * The count of watcher cycles since the pool start
     *
     * @return the watcherRuns
     */
    public synchronized int getWatcherRuns() {
        return watcherRuns;
    }

    /**
     * The count of opened connections to the database
     *
     * @return the openedConnections
     */
    public synchronized int getOpenedConnections() {
        return openedConnections;
    }

    /**
     * The Count of Thread Locks
     *
     * @return the threadLock
     */
    public synchronized int getThreadLock() {
        return threadLock;
    }

    /**
     * @return the maxPoolSizeReachedWhileFetching
     */
    public synchronized int getMaxPoolSizeReachedWhileFetching() {
        return maxPoolSizeReachedWhileFetching;
    }

    /**
     * @return the maxPoolSizeReached
     */
    public synchronized int getMaxPoolSizeReached() {
        return maxPoolSizeReached;
    }

    /**
     * @return the invalidConnection
     */
    public synchronized int getInvalidConnection() {
        return invalidConnection;
    }

    /**
     * @return the maxLifeTimeReached
     */
    public synchronized int getMaxLifeTimeReached() {
        return maxLifeTimeReached;
    }

    /**
     * @return the maxIdleTimeReached
     */
    public synchronized int getMaxIdleTimeReached() {
        return maxIdleTimeReached;
    }

    /**
     * @return the returnedToPool
     */
    public synchronized int getReturnedToPool() {
        return returnedToPool;
    }

    /**
     * @return the maxLoanedTimeReached
     */
    public synchronized int getMaxLoanedTimeReached() {
        return maxLoanedTimeReached;
    }
}