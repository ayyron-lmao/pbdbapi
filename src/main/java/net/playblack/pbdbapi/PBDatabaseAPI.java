package net.playblack.pbdbapi;

import java.io.File;
import java.util.logging.Logger;
import net.playblack.pbdbapi.config.ConnectionConfiguration;
import net.playblack.pbdbapi.config.DatabaseConfiguration;

/**
 *
 * @author somners
 */
public class PBDatabaseAPI {

    private static PBDatabaseAPI instance = null;
    private DatabaseConfiguration databaseConfig = null;
    private ConnectionConfiguration connectionConfig = null;
    private static Logger logger = Logger.getAnonymousLogger();
    private String configPath = null;

    /**
     * The default constructor for { @link PBDatabaseAPI }.  Creates a new
     * instance with all default values.
     */
    public PBDatabaseAPI() {
        this(null, null);
    }

    /**
     * Creates a new instance of PBDatabaseAPI with a custom configuration file
     * path location.  All other values will remain default.
     * @param configPath path to the configuration file for the database.
     */
    public PBDatabaseAPI(String configPath) {
        this(configPath, null);
    }

    /**
     * Creates a new instance of PBDatabaseAPI with a custom Logger for
     * PBDatabaseAPI to use.
     *
     * @param logger the logger to use to log messages.
     */
    public PBDatabaseAPI(Logger logger) {
        this(null, logger);
    }

    /**
     * Creates a new instance of PBDatabaseAPI with a custom Configuration Path and
     * Logger for PBDatabaseAPI to use.
     *
     * @param configPath path to the configuration file for the database.
     * @param logger the logger to use to log messages.
     */
    public PBDatabaseAPI(String configPath, Logger logger) {
        this.configPath = configPath != null ? configPath : "config" + File.separatorChar;
        PBDatabaseAPI.logger = logger != null ? logger : logger;
        databaseConfig = new DatabaseConfiguration(this.configPath + "db.cfg");
        connectionConfig = new ConnectionConfiguration(this.configPath + "connection.cfg");
    }

    /**
     * Gets the singleton instance of { @link PBDatabaseAPI }.
     * @return { @link PBDatabaseAPI }.
     */
    public static PBDatabaseAPI get() {
        if (instance == null) {
            instance = new PBDatabaseAPI();
        }
        return instance;
    }

    /**
     * Sets the { @link Logger } for the PBDatabaseAPI Library.
     * @param logger The { @link Logger } to set for the PBDatabaseAPI Library to use.
     * @return Returns the singleton for convenience.
     */
    public PBDatabaseAPI setLogger(Logger logger) {
        PBDatabaseAPI.logger = logger != null ? logger : logger;
        return instance;
    }

    /**
     * Gets the { @link Logger } for the PBDataAccess Library.
     * @return { @link Logger }
     */
    public static Logger logger() {
        return logger;
    }

    /**
     * Gets the path of the configuration file.
     * @return the path
     */
    public String getConfigPath() {
        return this.configPath;
    }

    /**
     * Gets the Database Configuration file.
     * @return { @link DatabaseConfiguration }.
     */
    public DatabaseConfiguration getDatabaseConfig() {
        return this.databaseConfig;
    }

    /**
     * Gets the Database Connection Configuration file.
     * @return { @link ConnectionConfiguration }.
     */
    public ConnectionConfiguration getConnectionConfig() {
        return this.connectionConfig;
    }
}
