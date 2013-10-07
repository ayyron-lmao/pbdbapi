package net.playblack.pbdbapi.queries;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author somners
 */
public abstract class Where<T extends Where> extends Query<T> {

    private List<QueryEntry> where = new ArrayList<QueryEntry>();
    private Object whereLock = new Object();
    private int limit = 1;

    /**
     * Adds a new Where Condition to this query. The API will search and select
     * columns that have the given value for the given column.
     *
     * @param column The name of the column we are looking to compare.
     * @param value The value of the given column name with which we want to
     * compare.
     * @return This Query for convenience.
     */
    public T where(String column, Object value) {
        synchronized(whereLock) {
            where.add(new QueryEntry(column, value));
        }
        return (T)this;
    }

    /**
     * Gets all the Where Conditions for this query. The API will search and select
     * columns that have the given values for the given columns.
     *
     * @return A list of Query Entries for Where Conditions.
     */
    public List<QueryEntry> getWheres() {
        List<QueryEntry> toRet = null;
        synchronized(whereLock) {
            toRet = new ArrayList<QueryEntry>(where);
        }
        return toRet;
    }

    /**
     * Sets the limit of rows returned from this query.
     *
     * @param limit
     * @return This Query for ease of use.
     */
    public T limit(int limit) {
        this.limit = limit;
        return (T)this;
    }

    /**
     * Gets the limit of rows returned from this query.
     *
     * @return The query return limit.
     */
    public int limit() {
        return limit;
    }
}
