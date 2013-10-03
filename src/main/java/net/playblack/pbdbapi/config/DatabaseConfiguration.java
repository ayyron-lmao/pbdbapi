package net.playblack.pbdbapi.config;

import java.io.File;
import java.util.logging.Level;
import net.playblack.pbdbapi.Database;
import net.playblack.pbdbapi.PBDatabaseAPI;
import net.visualillusionsent.utils.PropertiesFile;

/**
 * Database Configuration settings
 *
 * @author Jos Kuijpers
 * @author Jason (darkdiplomat)
 */
public class DatabaseConfiguration {
    private PropertiesFile cfg;

    public DatabaseConfiguration(String path) {
        File test = new File(path);

        if (!test.exists()) {
            PBDatabaseAPI.logger().log(Level.INFO, "Could not find the database configuration at " + path + ", creating default.");
        }
        this.cfg = new PropertiesFile(path);
        verifyConfig();
    }

    /** Reloads the configuration file */
    public void reload() {
        cfg.reload();
        verifyConfig();
    }

    /** Get the configuration file */
    public PropertiesFile getFile() {
        return cfg;
    }

    /** Creates the default configuration */
    private void verifyConfig() {
        cfg.getString("data-source", "xml");
        cfg.getString("database-path", "db");
        cfg.save();
    }

    /**
     * Get datasource type
     *
     * @return datasource type
     */
    public String getDatasourceType() {
        return cfg.getString("data-source", "xml");
    }

    /**
     * Gets the path of the database.
     * @return Returns the path of database.
     */
    public String getDatabasePath() {
        return cfg.getString("database-path", "db");
    }
}
