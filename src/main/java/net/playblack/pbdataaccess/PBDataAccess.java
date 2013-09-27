package net.playblack.pbdataaccess;

import java.io.File;
import java.util.logging.Logger;

/**
 *
 * @author somners
 */
public class PBDataAccess {
    
    private static PBDataAccess instance = null;
    private DatabaseConfiguration config = null;
    private static Logger logger = Logger.getAnonymousLogger();
    private String databaseType = null;
    private String configPath = null;
    private String databasePath = null;
    
    /**
     * The default constructor for { @link PBDataAccess }.  Creates a new 
     * instance with all default values.
     */
    public PBDataAccess() {
        this(null);
    }
    
    /**
     * Creates a new instance of PBDataAccess with a custom configuration file 
     * path location.  All other values will remain default.
     * @param configPath path to the configuration file for the database.
     */
    public PBDataAccess(String configPath) {
        this(configPath, null);
    }
       
    /**
     * Creates a new instance of PBDataAccess with a custom configuration file 
     * path location and default database type.  All other values will remain 
     * default.
     * @param configPath path to the configuration file for the database.
     * @param databaseType name of the database to use.
     */
    public PBDataAccess(String configPath, String databaseType) {
        this(configPath, databaseType, null);
    }
       
    /**
     * Creates a new instance of PBDataAccess with a custom configuration file 
     * path location and default database type and database File path (database 
     * file path is used for sqlite and xml).  All other values will remain 
     * default.
     * @param configPath path to the configuration file for the database.
     * @param databaseType name of the database to use.
     * @param databasePath path to database files (database file path is used 
     * for sqlite and xml)
     */
    public PBDataAccess(String configPath, String databaseType, String databasePath) {
        this.configPath = configPath != null ? configPath : "config" + File.separatorChar;
        this.databaseType = databaseType != null ? databaseType : "xml";
        this.databasePath = databasePath != null ? databasePath : "db" + File.separatorChar;
        this.config = new DatabaseConfiguration(this.configPath + "db.cfg");
    }
    
    /**
     * Gets the singleton instance of { @link PBDataAccess }.
     * @return { @link PBDataAccess }.
     */
    public static PBDataAccess get() {
        if (instance == null) {
            instance = new PBDataAccess();
        }
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
     * Sets the { @link Logger } for the PBDataAccess Library.
     * @param logger The { @link Logger } to set for the PBDataAccess Library to use.
     * @return Returns the singleton for convenience.
     */
    public PBDataAccess setLogger(Logger logger) {
        PBDataAccess.logger = logger != null ? logger : logger;
        return instance;
    }
    
    /**
     * Sets the path of the configuration file.
     * @param configPath path to the configuration file for the database.
     * @return Returns the singleton for convenience.
     */
    public PBDataAccess setConfigPath(String configPath) {
        if (configPath != null) {
            this.configPath = configPath;
        }
        return instance;
    }
    
    /**
     * Gets the path of the configuration file.
     * @return the path
     */
    public String getConfigPath() {
        return this.configPath;
    }
        
    /**
     * Sets the default database.
     * @param name name of the database to use by default
     * @return Returns the singleton for convenience.
     */
    public PBDataAccess setDatabaseType(String name) {
        if (name != null) {
            this.databaseType = name;
        }
        return instance;
    }
        
    /**
     * Gets the default database.
     * @return Returns the name of the default database.
     */
    public String getDatabaseType() {
        return databaseType;
    }
        
    /**
     * Sets the path of the database (only used for sqlite and xml).
     * @param name path of the database.
     * @return Returns the singleton for convenience.
     */
    public PBDataAccess setDatabasePath(String databasePath) {
        if (databasePath != null) {
            this.databasePath = databasePath;
        }
        return instance;
    }
        
    /**
     * Gets the path of the database.
     * @return Returns the path of database.
     */
    public String getDatabasePath() {
        return databasePath;
    }
    
    /**
     * Gets the Database Configuration file.
     * @return { @link DatabaseConfiguration }.
     */
    public DatabaseConfiguration getDatabaseConfig() {
        return this.config;
    }
}
