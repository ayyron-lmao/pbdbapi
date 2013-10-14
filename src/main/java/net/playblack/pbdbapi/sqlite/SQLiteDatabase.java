package net.playblack.pbdbapi.sqlite;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.playblack.pbdbapi.Column;
import net.playblack.pbdbapi.DataAccess;
import net.playblack.pbdbapi.Database;
import net.playblack.pbdbapi.JDBCHelper;
import net.playblack.pbdbapi.PBDatabaseAPI;
import net.playblack.pbdbapi.exceptions.DatabaseAccessException;
import net.playblack.pbdbapi.exceptions.DatabaseReadException;
import net.playblack.pbdbapi.exceptions.DatabaseTableInconsistencyException;
import net.playblack.pbdbapi.exceptions.DatabaseWriteException;
import net.playblack.pbdbapi.mysql.MySQLDatabase;
import net.playblack.pbdbapi.queries.Delete;
import net.playblack.pbdbapi.queries.Insert;
import net.playblack.pbdbapi.queries.Query;
import static net.playblack.pbdbapi.queries.Query.Type.DELETE;
import static net.playblack.pbdbapi.queries.Query.Type.INSERT;
import static net.playblack.pbdbapi.queries.Query.Type.SELECT;
import static net.playblack.pbdbapi.queries.Query.Type.UPDATE;
import static net.playblack.pbdbapi.queries.Query.Type.UPDATE_SCHEMA;
import net.playblack.pbdbapi.queries.QueryEntry;
import net.playblack.pbdbapi.queries.Select;
import net.playblack.pbdbapi.queries.Update;
import net.playblack.pbdbapi.queries.UpdateSchema;
import net.playblack.pbdbapi.queries.Where;

/**
 * SQLite Database
 *
 * @author Jason (darkdiplomat)
 */
public class SQLiteDatabase extends Database {

    private Connection conn; // One Connection, All the Time
    private static SQLiteDatabase instance;
    private final String LIST_REGEX = "\u00B6";
    private final String database;
    /** Takes: Table Name, Column Names, Values */
    private final String INSERT = "INSERT INTO `%s` (%s) VALUES (%s)";
    /** Takes: Table Name, Conditions, Limit */
    private final String SELECT = "SELECT * FROM `%s` WHERE %s LIMIT %s";
    /** Takes: Table Name, Limit */
    private final String SELECT_ALL = "SELECT * FROM `%s` LIMIT %s";
    /** Takes: Table Name, Column Data */
    private final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS `%s` (%s) ENGINE = INNODB";
    /** Takes: Table Name, Column Name, JDBC Data Type Syntax */
    private final String INSERT_COLUMN = "ALTER TABLE `%s` ADD `%s` %s";
    /** Takes: Table Name, Column Name */
    private final String DELETE_COLUMN = "ALTER TABLE `%s` DROP `%s`";

    private SQLiteDatabase() {
        File path = new File("db/");

        if (!path.exists()) {
            path.mkdirs();
        }
        database = PBDatabaseAPI.get().getConnectionConfig().getDatabaseName();
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + database + ".db");
        }
        catch (Exception ex) {
            PBDatabaseAPI.logger().log(Level.WARNING, "Failed to create connection to SQLite database", ex);
        }
    }

    public static SQLiteDatabase getInstance() {
        if (SQLiteDatabase.instance == null) {
            SQLiteDatabase.instance = new SQLiteDatabase();
        }
        return SQLiteDatabase.instance;
    }

    @Override
    public DataAccess[] query(Select query) throws DatabaseReadException {
        List<DataAccess> toRet = this.load(query, conn);
        return toRet.toArray(new DataAccess[toRet.size()]);
    }

    @Override
    public void executeQueries() throws DatabaseWriteException {
        try {
            conn.setAutoCommit(false);
            synchronized (lock) {
                for (Query query : super.queue) {
                    switch(query.getType()) {
                        case DELETE:
                            this.remove((Delete) query, conn);
                            break;
                        case INSERT:
                            this.insert((Insert) query, conn);
                            break;
                        case UPDATE:
                            this.update((Update) query, conn);
                            break;
                        case SELECT:
                            // Aren't capable of returning anything, so just skip it.
                            break;
                        case UPDATE_SCHEMA:
                            this.updateSchema((UpdateSchema) query, conn);
                            break;
                    }
                }
            }
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void updateSchema(UpdateSchema... udpateSchema) throws DatabaseWriteException {
        for (Query query : udpateSchema) {
            this.updateSchema((UpdateSchema) query, conn);
        }
    }

    public void insert(Insert query, Connection conn) throws DatabaseWriteException {
        if (this.doesEntryExist(conn, query.from())) {
            return;
        }
        PreparedStatement ps = null;

        try {
            StringBuilder fields = new StringBuilder(), values = new StringBuilder();
            HashMap<Column, Object> columns = query.from().toDatabaseEntryList();
            Iterator<Column> it = columns.keySet().iterator();

            /* Generates field and value Strings */
            Column column;
            while (it.hasNext()) {
                column = it.next();
                if (!column.autoIncrement()) {
                    fields.append("`").append(column.columnName()).append("`").append(",");
                    values.append("?").append(",");
                }
            }
            /* Deletes the trailing comma's for proper syntax */
            fields.deleteCharAt(fields.length() - 1);
            values.deleteCharAt(values.length() - 1);

            ps = conn.prepareStatement(String.format(INSERT, query.from().getName(), fields, values));

            /* Inserts values to columns */
            int i = 1;
            for (Column c : columns.keySet()) {
                if (!c.autoIncrement()) {
                    if (c.isList()) {
                        ps.setObject(i, JDBCHelper.getListString((List<?>) columns.get(c)));
                    }
                    else {
                        ps.setObject(i, JDBCHelper.convert(columns.get(c)));
                    }
                    i++;
                }
            }

            if (ps.executeUpdate() == 0) {
                throw new DatabaseWriteException("Error inserting MySQL: no rows updated!");
            }
        }
        catch (SQLException ex) {
            PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
        }
        catch (DatabaseTableInconsistencyException dtie) {
            PBDatabaseAPI.logger().log(Level.WARNING, dtie.getMessage(), dtie);
        }
        finally {
            this.closePS(ps);
        }
    }

    public void update(Update query, Connection conn) throws DatabaseWriteException {
        if (!this.doesEntryExist(conn, query.from())) {
            return;
        }
        ResultSet rs = null;

        try {
            HashMap<Column, Object> columns = query.from().toDatabaseEntryList();
            Select select = this.getSelectFromWhere(query);

            rs = this.getResultSet(conn, select);

            if (rs != null) {
                if (rs.next()) {
                    Iterator<Column> it = columns.keySet().iterator();
                    Column column;
                    while (it.hasNext()) {
                        column = it.next();
                        if (column.isList()) {
                            rs.updateObject(column.columnName(), JDBCHelper.getListString((List<?>) columns.get(column)));
                        }
                        else {
                            rs.updateObject(column.columnName(), columns.get(column));
                        }
                    }
                    rs.updateRow();
                }
                else {
                    throw new DatabaseWriteException("Error updating DataAccess to MySQL, no such entry: " + query.from().toString());
                }
            }
        }
        catch (SQLException ex) {
            PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
        }
        catch (DatabaseTableInconsistencyException dtie) {
            PBDatabaseAPI.logger().log(Level.WARNING, dtie.getMessage(), dtie);
        }
        catch (DatabaseReadException ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            try {
                PreparedStatement st = rs != null && rs.getStatement() instanceof PreparedStatement ? (PreparedStatement) rs.getStatement() : null;
                this.closeRS(rs);
                this.closePS(st);
            }
            catch (SQLException ex) {
                PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }

    public void remove(Delete query, Connection conn) throws DatabaseWriteException {
        ResultSet rs = null;

        try {
            HashMap<Column, Object> columns = query.from().toDatabaseEntryList();
            Select select = this.getSelectFromWhere(query);
            rs = this.getResultSet(conn, select);

            if (rs != null) {
                if (rs.next()) {
                    rs.deleteRow();
                }
            }

        }
        catch (DatabaseReadException dre) {
            PBDatabaseAPI.logger().log(Level.WARNING, dre.getMessage(), dre);
        }
        catch (SQLException ex) {
            PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
        }
        catch (DatabaseTableInconsistencyException ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            try {
                PreparedStatement st = rs != null && rs.getStatement() instanceof PreparedStatement ? (PreparedStatement) rs.getStatement() : null;
                this.closeRS(rs);
                this.closePS(st);
            }
            catch (SQLException ex) {
                PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }

    public List<DataAccess> load(Select query, Connection conn) throws DatabaseReadException {
        List<DataAccess> toRet = new ArrayList<DataAccess>();
        ResultSet rs = null;
        HashMap<String, Object> dataSet = new HashMap<String, Object>();
        try {
            rs = this.getResultSet(conn, query);

            if (rs != null) {
                while (rs.next()) {


                    for (Column column : query.from().getTableLayout()) {
                        if (column.isList()) {
                            dataSet.put(column.columnName(), JDBCHelper.getList(column.dataType(), rs.getString(column.columnName())));
                        }
                        else if (rs.getObject(column.columnName()) instanceof Boolean) {
                            dataSet.put(column.columnName(), rs.getBoolean(column.columnName()));
                        }
                        else {
                            dataSet.put(column.columnName(), rs.getObject(column.columnName()));
                        }
                    }

                    DataAccess access = null;
                    access = query.from().getClass().newInstance();
                    access.load(dataSet);
                    toRet.add(access);
                    dataSet.clear(); // Don't forget to clear the hashmap for the next dataset :)
                }
            }
        }
        catch (DatabaseReadException dre) {
            PBDatabaseAPI.logger().log(Level.WARNING, dre.getMessage(), dre);
        }
        catch (SQLException ex) {
            PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
        }
        catch (DatabaseTableInconsistencyException dtie) {
            PBDatabaseAPI.logger().log(Level.WARNING, dtie.getMessage(), dtie);
        }
        catch (DatabaseAccessException ex) {
            PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
        }
        catch (InstantiationException ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (IllegalAccessException ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            try {
                PreparedStatement st = rs != null && rs.getStatement() instanceof PreparedStatement ? (PreparedStatement) rs.getStatement() : null;
                this.closeRS(rs);
                this.closePS(st);
            }
            catch (SQLException ex) {
                PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return toRet;
    }

    public void updateSchema(UpdateSchema query, Connection conn) throws DatabaseWriteException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            // First check if the table exists, if it doesn't we'll skip the rest
            // of this method since we're creating it fresh.
            DatabaseMetaData metadata = conn.getMetaData();
            rs = metadata.getTables(null, null, query.from().getName(), null);
            if (!rs.first()) {
                this.createTable(conn, query.from());
            }
            else {

                LinkedList<String> toRemove = new LinkedList<String>();
                HashMap<String, Column> toAdd = new HashMap<String, Column>();
                Iterator<Column> it = query.from().getTableLayout().iterator();

                Column column;
                while (it.hasNext()) {
                    column = it.next();
                    toAdd.put(column.columnName(), column);
                }

                for (String col : this.getColumnNames(query.from())) {
                    if (!toAdd.containsKey(col)) {
                        toRemove.add(col);
                    }
                    else {
                        toAdd.remove(col);
                    }
                }

                for (String name : toRemove) {
                    this.deleteColumn(conn, query.from().getName(), name);
                }
                for (Map.Entry<String, Column> entry : toAdd.entrySet()) {
                    this.insertColumn(conn, query.from().getName(), entry.getValue());
                }
            }
        }
        catch (SQLException sqle) {
            throw new DatabaseWriteException("Error updating MySQL schema: " + sqle.getMessage());
        }
        catch (DatabaseTableInconsistencyException dtie) {
            PBDatabaseAPI.logger().log(Level.WARNING, "Error updating MySQL schema." + dtie.getMessage(), dtie);
        }
        finally {
            this.closeRS(rs);
            this.closePS(ps);
        }
    }

    public ResultSet getResultSet(Connection conn, Select select) throws DatabaseReadException {
        PreparedStatement ps;
        ResultSet toRet = null;

        try {
            if (select.getWheres().size() > 0) {
                StringBuilder sb = new StringBuilder();

                for (QueryEntry entry : select.getWheres()) {
                    sb.append(" AND `").append(entry.getColumnName()).append("`=?");
                }
                sb.delete(0, 5);

                ps = conn.prepareStatement(String.format(SELECT, select.from().getName(), sb, select.limit()));

                int i = 0;
                for (QueryEntry entry : select.getWheres()) {
                    ps.setObject(i + 1, JDBCHelper.convert(entry.getColumnValue()));
                    i++;
                }
                toRet = ps.executeQuery();
            }
            else {
                ps = conn.prepareStatement(String.format(SELECT_ALL, select.from().getName(), select.limit()));

                toRet = ps.executeQuery();
            }
        }
        catch (SQLException ex) {
            throw new DatabaseReadException("Error Querying MySQL ResultSet in "
                    + select.from().getName());
        }
        catch (Exception ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        return toRet;
    }

    public boolean doesEntryExist(Connection conn, DataAccess data) throws DatabaseWriteException {
//        Connection conn = pool.getConnectionFromPool();
        ResultSet rs = null;
        boolean toRet = false;

        try {
            HashMap<Column, Object> columns = data.toDatabaseEntryList();
            Select select = (Select)Query.Type.SELECT.newQuery();
            select.from(data).limit(1);
            for (Column c : columns.keySet()) {
                select.where(c.columnName(), columns.get(c.columnName()));
            }

            rs = this.getResultSet(conn, select);
            toRet = rs.next();
        }
        catch (SQLException ex) {
            throw new DatabaseWriteException(ex.getMessage() + " Error checking MySQL Entry Key in "
                    + data.toString());
        }
        catch (DatabaseTableInconsistencyException ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        } catch (DatabaseReadException ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            this.closeRS(rs);
        }
        return toRet;
    }

    public List<String> getColumnNames(DataAccess data) {
        Statement statement = null;
        ResultSet resultSet = null;

        ArrayList<String> columns = new ArrayList<String>();
        String columnName;

        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery("SHOW COLUMNS FROM `" + data.getName() + "`");
            while (resultSet.next()) {
                columnName = resultSet.getString("field");
                columns.add(columnName);
            }
        }
        catch (SQLException ex) {
            PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage(), ex);
        }
        finally {
            this.closeRS(resultSet);
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException ex) {
                    Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return columns;
    }

    public void createTable(Connection conn, DataAccess data) throws DatabaseWriteException {
        PreparedStatement ps = null;

        try {
            StringBuilder fields = new StringBuilder();
            HashMap<Column, Object> columns = data.toDatabaseEntryList();
            Iterator<Column> it = columns.keySet().iterator();
            String primary = null;

            Column column;
            while (it.hasNext()) {
                column = it.next();
                fields.append("`").append(column.columnName()).append("` ");
                fields.append(JDBCHelper.getDataTypeSyntax(column.dataType()));
                if (column.autoIncrement()) {
                    fields.append(" AUTO_INCREMENT");
                }
                if (column.columnType().equals(Column.ColumnType.PRIMARY)) {
                    primary = column.columnName();
                }
                if (it.hasNext()) {
                    fields.append(", ");
                }
            }
            if (primary != null) {
                fields.append(", PRIMARY KEY(`").append(primary).append("`)");
            }
            ps = conn.prepareStatement(String.format(CREATE_TABLE, data.getName(), fields.toString()));
            ps.execute();
        }
        catch (SQLException ex) {
            throw new DatabaseWriteException("Error creating MySQL table '" + data.getName() + "'. " + ex.getMessage());
        }
        catch (DatabaseTableInconsistencyException ex) {
            PBDatabaseAPI.logger().log(Level.WARNING, ex.getMessage() + " Error creating MySQL table '" + data.getName() + "'. ", ex);
        }
        finally {
            this.closePS(ps);
        }
    }

    public void insertColumn(Connection conn, String tableName, Column column) throws DatabaseWriteException {
        PreparedStatement ps = null;

        try {
            if (column != null && !column.columnName().trim().equals("")) {
                ps = conn.prepareStatement(String.format(DELETE_COLUMN, tableName, column.columnName(), JDBCHelper.getDataTypeSyntax(column.dataType())));
                ps.execute();
            }
        }
        catch (SQLException ex) {
            throw new DatabaseWriteException("Error adding MySQL collumn: " + column.columnName());
        }
        finally {
            this.closePS(ps);
        }

    }

    public void deleteColumn(Connection conn, String tableName, String columnName) throws DatabaseWriteException {
        PreparedStatement ps = null;

        try {
            if (columnName != null && !columnName.trim().equals("")) {
                ps = conn.prepareStatement(String.format(DELETE_COLUMN, tableName, columnName));
                ps.execute();
            }
        }
        catch (SQLException ex) {
            throw new DatabaseWriteException("Error deleting MySQL collumn: " + columnName);
        }
        finally {
            this.closePS(ps);
        }
    }

    public Select getSelectFromWhere(Where where) {
        Select select = null;
        try {
            HashMap<Column, Object> columns = where.from().toDatabaseEntryList();
            select = (Select)Query.Type.SELECT.newQuery();
            select.from(where.from()).limit(1);
            for (Column c : columns.keySet()) {
                select.where(c.columnName(), columns.get(c.columnName()));
            }
        } catch (DatabaseTableInconsistencyException ex) {
            Logger.getLogger(MySQLDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        return select;
    }

    /**
     * Safely Close a ResultSet.
     *
     * @param rs
     *         ResultSet to close.
     */
    public void closeRS(ResultSet rs) {
        if (rs != null) {
            try {
                if (!rs.isClosed()) {
                    rs.close();
                }
            }
            catch (SQLException sqle) {
                PBDatabaseAPI.logger().log(Level.WARNING, "Error closing ResultSet in MySQL database.", sqle);
            }
        }
    }

    /**
     * Safely Close a PreparedStatement.
     *
     * @param ps
     *         PreparedStatement to close.
     */
    public void closePS(Statement ps) {
        if (ps != null) {
            try {
                if (!ps.isClosed()) {
                    ps.close();
                }
            }
            catch (SQLException sqle) {
                PBDatabaseAPI.logger().log(Level.WARNING, "Error closing PreparedStatement in MySQL database.", sqle);
            }
        }
    }

    /**
     * Safely Close a PreparedStatement.
     *
     * @param ps
     *         PreparedStatement to close.
     */
    public void closePS(ResultSet rs) {
        if (rs != null) {
            try {
                if (!rs.isClosed()) {
                    this.closePS(rs.getStatement());
                }
            }
            catch (SQLException sqle) {
                PBDatabaseAPI.logger().log(Level.WARNING, "Error closing PreparedStatement in MySQL database.", sqle);
            }
        }
    }
}
