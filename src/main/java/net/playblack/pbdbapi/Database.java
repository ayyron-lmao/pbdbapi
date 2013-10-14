package net.playblack.pbdbapi;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import net.playblack.pbdbapi.exceptions.DatabaseException;
import net.playblack.pbdbapi.exceptions.DatabaseReadException;
import net.playblack.pbdbapi.exceptions.DatabaseWriteException;
import net.playblack.pbdbapi.mysql.MySQLDatabase;
import net.playblack.pbdbapi.queries.Delete;
import net.playblack.pbdbapi.queries.Insert;
import net.playblack.pbdbapi.queries.Query;
import net.playblack.pbdbapi.queries.Select;
import net.playblack.pbdbapi.queries.Update;
import net.playblack.pbdbapi.queries.UpdateSchema;
import net.playblack.pbdbapi.sqlite.SQLiteDatabase;
import net.playblack.pbdbapi.xml.XmlDatabase;

/**
 * A database representation, used to store any kind of data
 *
 * @author chris
 */
public abstract class Database {

    /**
     * The datasource type
     *
     * @author chris
     */
    public static class Type {
        private static HashMap<String, Database> registeredDatabases = new HashMap<String, Database>();

        public static void registerDatabase(String name, Database db) throws DatabaseException {
            if (registeredDatabases.containsKey(name)) {
                throw new DatabaseException(name + " cannot be registered. Type already exists");
            }
            registeredDatabases.put(name, db);
        }

        public static Database getDatabaseFromType(String name) {
            return registeredDatabases.get(name);
        }

        static {
            try {
                Database.Type.registerDatabase("xml", XmlDatabase.getInstance());
                Database.Type.registerDatabase("mysql", MySQLDatabase.getInstance());
                Database.Type.registerDatabase("sqlite", SQLiteDatabase.getInstance());
            }
            catch (DatabaseException e) {
            }
        }
    }

    protected static LinkedList<Query> queue = new LinkedList<Query>();
    protected static final Object lock = new Object();

    public static Database get() {
        Database ret = Database.Type.getDatabaseFromType(PBDatabaseAPI.get().getDatabaseConfig().getDatasourceType());
        if (ret != null) {
            return ret;
        }
        else {
            PBDatabaseAPI.logger().log(Level.WARNING, "Database type " + Database.Type.getDatabaseFromType(PBDatabaseAPI.get().getDatabaseConfig().getDatasourceType()) + " is not available, falling back to XML! Fix your server.cfg");
            return XmlDatabase.getInstance();
        }
    }

    /**
     * Creates a new Select Query.
     * @return A new Query Instance.
     */
    public Select select() {
        return (Select) Query.Type.SELECT.newQuery();
    }

    /**
     * Creates a new Delete Query.
     * @return A new Query Instance.
     */
    public Delete delete() {
        return (Delete) Query.Type.DELETE.newQuery();
    }

    /**
     * Creates a new Insert Query.
     * @return A new Query Instance.
     */
    public Insert insert() {
        return (Insert) Query.Type.INSERT.newQuery();
    }

    /**
     * Creates a new Update Query.
     * @return A new Query Instance.
     */
    public Update update() {
        return (Update) Query.Type.UPDATE.newQuery();
    }

    /**
     * Creates a new Schema Update Query.
     * @return A new Query Instance.
     */
    public UpdateSchema updateSchema() {
        return (UpdateSchema) Query.Type.UPDATE_SCHEMA.newQuery();
    }



    /**
     * Executes a Read query from the database assortment of { @link Query } objects.
     *
     * @param query
     *         the queries to execute.
     *
     * @throws DatabaseReadException
     *         when something went wrong during the write operation
     */
    public abstract DataAccess[] query(Select query) throws DatabaseReadException;

    /**
     * Queues a write query to be executed during a data dump.
     *
     * @param query
     *         the queries to queue.
     */
    public void queueQuery(Query... query) {
        synchronized (lock) {
            if (query != null) {
                queue.addAll(Arrays.asList(query));
            }
        }
    }

    /**
     * Executes all the queued Write Queries.
     *
     * @throws DatabaseWriteException
     *         when something went wrong during the write operation
     */
    public abstract void executeQueries() throws DatabaseWriteException;

    /**
     * Updates the database table fields for the given { @link UpdateSchema } object.
     * This method will remove fields that aren't there anymore and add new ones if applicable.
     *
     * @param updateSchema
     *         the new schema update.
     */
    public abstract void updateSchema(UpdateSchema... udpateSchema) throws DatabaseWriteException;
}
