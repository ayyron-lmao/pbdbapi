package net.playblack.pbdbapi.queries;

/**
 *
 * @author somners
 */
public final class QueryEntry {
    
    private String column = null;
    private Object value = null;
    
    public QueryEntry(String column, Object value) {
        this.column = column;
        this.value = value;
    }
    
    /**
     * Gets the name of the column.
     * 
     * @return The Column name.
     */
    public String getColumnName() {
        return this.column;
    }
    
    /**
     * Gets the value for the column.
     * 
     * @return The column value.
     */
    public Object getColumnValue() {
        return this.column;
    }
}
