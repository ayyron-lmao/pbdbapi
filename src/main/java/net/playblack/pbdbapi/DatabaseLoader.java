package net.playblack.pbdbapi;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.logging.Level;

import net.playblack.pbdbapi.exceptions.DatabaseException;
import net.visualillusionsent.utils.PropertiesFile;
import net.visualillusionsent.utils.UtilityException;

/**
 * Checks a database folder in CanaryMods root folder for
 * external Database Implementations and loads them
 *
 * @author chris
 */
public class DatabaseLoader {
    /**
     * Scans database folder, loads all valid databases and registers them
     * at Database.Type. This must be the first bootstrapping step,
     * as all other steps require a functional database.
     * This also means this must not make use of anything that isn't loaded already
     */
    public static void load() {
        File dbFolder = new File("databases/");
        if (!dbFolder.exists()) {
            dbFolder.mkdirs();
        }
        for (File file : dbFolder.listFiles()) {
            if (!file.getName().endsWith(".jar")) {
                continue;
            }
            URLClassLoader loader;
            try {
                loader = new URLClassLoader(new URL[] {file.toURI().toURL()}, Thread.currentThread().getContextClassLoader());
            }
            catch (MalformedURLException ex) {
                Database.logger().log(Level.WARNING, "Exception while loading database jar", ex);
                return;
            }
            PropertiesFile inf = new PropertiesFile(file.getAbsolutePath(), "Canary.inf");
            try {
                String mainclass = inf.getString("main-class");
                String dbName = inf.getString("database-name");
                Class<?> dbClass = loader.loadClass(mainclass);
                Method m = dbClass.getMethod("getInstance", new Class[0]);
                Database db = (Database) m.invoke(dbClass, new Object[0]);
                if (db != null) {
                    Database.Type.registerDatabase(dbName, db);
                }
            }
            catch (UtilityException e) {
                Database.logger().log(Level.WARNING, "Could not find databases mainclass", e);
                return;
            }
            catch (ClassNotFoundException e) {
                Database.logger().log(Level.WARNING, "Could not find databases mainclass", e);
            }
            catch (IllegalAccessException e) {
                Database.logger().log(Level.WARNING, "Could not create database", e);
            }
            catch (DatabaseException e) {
                Database.logger().log(Level.WARNING, "Could not add database", e);
            }
            catch (SecurityException e) {
                Database.logger().log(Level.WARNING, e.getMessage(), e);
            }
            catch (NoSuchMethodException e) {
                Database.logger().log(Level.WARNING, "Database does not contain a static getInstance() method!", e);
            }
            catch (IllegalArgumentException e) {
                Database.logger().log(Level.WARNING, e.getMessage(), e);
            }
            catch (InvocationTargetException e) {
                Database.logger().log(Level.WARNING, e.getMessage(), e);
            }
        }
    }
}
