package net.playblack.pbdbapi.queries;

import java.util.logging.Level;
import net.playblack.pbdbapi.DataAccess;
import net.playblack.pbdbapi.PBDatabaseAPI;

/**
 *
 * @author somners
 */
public abstract class Query<T extends Query> {

    public enum Type {

        DELETE(Delete.class),
        INSERT(Insert.class),
        SELECT(Select.class),
        UPDATE(Update.class),
        UPDATE_SCHEMA(UpdateSchema.class);

        private Class<? extends Query> query;

        Type(Class<? extends Query> query) {
            this.query = query;
        }

        public Query newQuery() {
            Query toRet = null;
            try {
                toRet = query.newInstance();
            } catch (InstantiationException ex) {
                PBDatabaseAPI.logger().log(Level.WARNING, "Error creating new instance of " + query.getName(), ex);
            } catch (IllegalAccessException ex) {
                PBDatabaseAPI.logger().log(Level.WARNING, "Error creating new instance of " + query.getName(), ex);
            }
            return null;
        }

    }

    protected DataAccess access = null;

    /**
     * Sets the Database Table/Schema to query.
     *
     * @param access The { @link DataAccess } representing the Table/Schema to
     * query.
     * @return This Query for convenience.
     */
    public T from(DataAccess access) {
        this.access = access;
        return (T)this;
    }

    /**
     * Gets the Database Table/Schema to query.
     *
     * @return The DataAccess to Query.
     */
    public DataAccess from() {
        return access;
    }

    /**
     * Gets the { @link Query.Type } of this Query.
     *
     * @return { @link Query.Type } of this Query.
     */
    public abstract Type getType();
}
