package net.playblack.pbdataaccess.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.logging.Level;
import net.playblack.pbdataaccess.DatabaseConfiguration;
import net.playblack.pbdataaccess.PBDataAccess;


/**
 * This class is a MySQL Connection Pool for the MySQL backend for CanaryMod.
 * Please Note that you must return all connections used to the pool in order
 * for this to serve any purpose.
 *
 * @author Somners
 */
public class MySQLConnectionPool {

    private DatabaseConfiguration config;
    private LinkedList<Connection> connectionPool;

    public MySQLConnectionPool() {
        // Only establish the data and connections of the configuration is valid
        if (!PBDataAccess.get().getDatabaseType().equalsIgnoreCase("mysql")) {
            return;
        }
        config = PBDataAccess.get().getDatabaseConfig();
        connectionPool = new LinkedList<Connection>();
        this.initializeConnectionPool();
    }

    /** Creates the connection pool. */
    private void initializeConnectionPool() {
        PBDataAccess.logger().log(Level.INFO, "Creating MySQL Connection pool.");
        while (!this.isConnectionPoolFull()) {
            this.addNewConnectionToPool();
        }
        PBDataAccess.logger().log(Level.INFO, "Finished creating MySQL Connection pool.");
    }

    /**
     * Checks if the connection pool is full.
     *
     * @return true - pool is full<br>
     *         false - pool is not full
     */
    private synchronized boolean isConnectionPoolFull() {
        return connectionPool.size() > config.getDatabaseMaxConnections();
    }

    /**
     * Checks if the connection pool is empty.
     *
     * @return true - pool is empty<br>
     *         false - pool is not empty
     */
    private synchronized boolean isConnectionPoolEmpty() {
        return connectionPool.isEmpty();
    }

    /** Adds a new Connection to the pool. */
    private void addNewConnectionToPool() {
        Connection connection;

        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            connection = DriverManager.getConnection(config.getDatabaseUrl("mysql"), config.getDatabaseUser(), config.getDatabasePassword());
            if (connection.isValid(5)) {
                connectionPool.addLast(connection);
            }
        }
        catch (SQLException sqle) {
            PBDataAccess.logger().log(Level.WARNING, "SQLException Adding Connection to MySQL Pool.", sqle);
        }
        catch (ClassNotFoundException cnfe) {
            PBDataAccess.logger().log(Level.WARNING, "ClassNotFoundException Adding Connection to MySQL Pool.", cnfe);
        }
        catch (InstantiationException ie) {
            PBDataAccess.logger().log(Level.WARNING, "InstantiationException Adding Connection to MySQL Pool.", ie);
        }
        catch (IllegalAccessException iae) {
            PBDataAccess.logger().log(Level.WARNING, "IllegalAccessException Adding Connection to MySQL Pool.", iae);
        }
    }

    /**
     * Gets a Connection from the pool. Remember to return it!
     *
     * @return A connection from the pool.
     *
     * @see MySQLConnectionPool#returnConnectionToPool(Connection)
     */
    public synchronized Connection getConnectionFromPool() {
        if (this.isConnectionPoolEmpty()) {
            this.addNewConnectionToPool();
            PBDataAccess.logger().log(Level.WARNING, "Adding new connection to MySQL connection " + "pool. Why are you running out of connections?");
        }

        return connectionPool.removeFirst();
    }

    /**
     * Returns a connection to the pool.
     *
     * @param connection
     *         The connection to return.
     */
    public synchronized void returnConnectionToPool(Connection connection) {
        if (!this.isConnectionPoolFull()) {
            connectionPool.add(connection);
        }
        else {
            try {
                connection.close();
            }
            catch (SQLException sqle) {
                PBDataAccess.logger().log(Level.WARNING, "SQLException closing MySQL Connection.", sqle);
            }
        }
    }

    /** Closes all connections in the pool and recreates all connections. */
    public synchronized void flushAndRefillConnectionPool() {
        for (Connection conn : connectionPool) {
            try {
                conn.close();
            }
            catch (SQLException sqle) {
                PBDataAccess.logger().log(Level.WARNING, "SQLException closing MySQL Connection.", sqle);
            }
        }
        connectionPool = null;
        this.initializeConnectionPool();
    }
}
