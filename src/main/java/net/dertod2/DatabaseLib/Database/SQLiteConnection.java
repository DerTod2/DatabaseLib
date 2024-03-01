package net.dertod2.DatabaseLib.Database;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class SQLiteConnection implements Connection {
    protected final Connection rawConnection;

    public SQLiteConnection(Connection connection) {
        this.rawConnection = connection;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.rawConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.rawConnection.isWrapperFor(iface);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return this.rawConnection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return this.rawConnection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return this.rawConnection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return this.rawConnection.nativeSQL(sql);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.rawConnection.getAutoCommit();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.rawConnection.setAutoCommit(autoCommit);
    }

    @Override
    public void commit() throws SQLException {
        this.rawConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        this.rawConnection.rollback();
    }

    @Override
    public void close() {
        // Not for SQLite Connections
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.rawConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return this.rawConnection.getMetaData();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.rawConnection.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.rawConnection.setReadOnly(readOnly);
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.rawConnection.getCatalog();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.rawConnection.setCatalog(catalog);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.rawConnection.getTransactionIsolation();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.rawConnection.setTransactionIsolation(level);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.rawConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.rawConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.rawConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.rawConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.rawConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return this.rawConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.rawConnection.setTypeMap(map);
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.rawConnection.getHoldability();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        this.rawConnection.setHoldability(holdability);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return this.rawConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return this.rawConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        this.rawConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        this.rawConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return this.rawConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return this.rawConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return this.rawConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return this.rawConnection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return this.rawConnection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return this.rawConnection.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return this.rawConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return this.rawConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return this.rawConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return this.rawConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this.rawConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        this.rawConnection.setClientInfo(name, value);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return this.rawConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return this.rawConnection.getClientInfo();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.rawConnection.setClientInfo(properties);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return this.rawConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return this.rawConnection.createStruct(typeName, attributes);
    }

    @Override
    public String getSchema() throws SQLException {
        return this.rawConnection.getSchema();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.rawConnection.setSchema(schema);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.rawConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        this.rawConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return this.rawConnection.getNetworkTimeout();
    }
}