package com.jedou.common.cli.jdbc;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class JdbcQuery extends SqlQuery<JdbcQuery> {

    public static boolean show_sql = false;
    public static boolean show_param = false;

    private int limit         = 0;
    private int offset        = 0;
    private int page          = 0;

    private String executeSQL = null;

    public JdbcQuery limit(int limit) {
        this.limit = limit;
        return this;
    }
    public JdbcQuery offset(int offset) {
        this.offset = offset;
        return this;
    }
    public JdbcQuery page(int page){
        this.page = page;
        return this;
    }
    public JdbcQuery select() {
        this.concat("select");
        return this;
    }
    public JdbcQuery insert() {
        this.concat("insert into");
        return this;
    }
    public JdbcQuery update() {
        this.concat("update");
        return this;
    }
    public JdbcQuery delete() {
        this.concat("delete");
        return this;
    }

    public static class ParamsStatementCallback {

        Object[] params = null;

        public ParamsStatementCallback(Object[] params) {
            this.params = params;
        }

        public Object doInPreparedStatement(PreparedStatement stmt)
                throws SQLException {
            for (int i = 0; i < params.length; i++) {
                if (params[i] instanceof java.util.Date)
                    stmt.setTimestamp(i+1, new Timestamp(((Date)params[i]).getTime()));
                else
                    stmt.setObject(i+1, params[i]);
            }
            return fetchList(stmt.executeQuery());
        }

        public List<Map<String, Object>> fetchList(ResultSet rs) throws SQLException {
            List<Map<String, Object>> rsList = new ArrayList<Map<String, Object>>();
            ResultSetMetaData rsma = rs.getMetaData();
            int colNum = rsma.getColumnCount();
            try {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<String, Object>();
                    for (int i = 1; i <= colNum; i++) {
                        row.put(rsma.getColumnName(i).toLowerCase(), rs.getObject(i));
                    }
                    rsList.add(row);
                }
            }
            finally {
                if (rs != null) rs.close();
            }
            return rsList;
        }
    }

    public static class JdbcTemplate {
        public static JdbcTemplate New() {
            return new JdbcTemplate();
        }
        public Object execute(String sql, ParamsStatementCallback callback) throws SQLException {
            PreparedStatement pstmt = DB.getConnection().prepareStatement(sql);
            try {
                return callback.doInPreparedStatement(pstmt);
            }
            finally {
                if (pstmt != null) pstmt.close();
            }
        }
        @SuppressWarnings("finally")
        public int executeUpdate(String sql, Object...params) throws SQLException {
            PreparedStatement pstmt = DB.getConnection().prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++)
                    pstmt.setObject(i+1, params[i]);
            }

            int num = 0;

            try {
                pstmt.executeUpdate();
            }catch(Throwable e){
                e.printStackTrace();
                throw new Exception(e.getMessage());
            }
            finally{
                if(pstmt != null)
                    pstmt.close();

                return num;
            }
        }
    }

    public Object executeQuery() {
        String _sql = executeSQL != null ? executeSQL : sql.toString();
        return executeQuery(_sql, params.toArray());
    }
    public Object executeQuery(String _sql, Object...params) {
        try {
            if (show_sql) System.out.println("[SQL] " + _sql);
            if (show_param && params != null) {
                for (int i = 0; i < params.length; i++) {
                    Object o = params[i];
                    System.out.println("[SQL params] "+i+": "+o);
                }
            }

            return JdbcTemplate.New().execute(_sql, new ParamsStatementCallback(params));
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    public Map<String, Object> firstRow() {
        return firstRow(params.toArray());
    }
    public Map<String, Object> firstRow(Object...params) {
        if (useDialect().supportsLimit()) {
            List array = new ArrayList();

            if (params != null) {
                for (Object p : params) {
                    if (p != null) array.add(p);
                }
            }

            String executeSQL = useDialect().getLimitString(sql.toString(), false);
            array.add(1);

            List<Map<String, Object>> rows = (List<Map<String, Object>>)executeQuery(executeSQL, array.toArray());
            if (rows.size() > 0)
                return rows.get(0);
            else
                return null;
        }
        else
            throw new RuntimeException("Database not support limit clause: " + useDialect());
    }
    public List<Map<String, Object>> asList() {
        return asList(params.toArray());
    }
    public List<Map<String, Object>> asList(Object...params) {
        try {
            List array = new ArrayList();
            if (params != null) {
                for (Object p : params) {
                    if (p != null) array.add(p);
                }
            }
            //计算offset
            if (this.page > 0){
                int firstRow = (page - 1) * limit;
                this.offset = firstRow < 0?0:firstRow;
            }
            if ((limit > 0 || offset > 0) && useDialect().supportsLimit()) {
                executeSQL = useDialect().getLimitString(sql.toString(), offset > 0);
                if (limit > 0 && offset == 0)
                    array.add(useDialect().getLimitValue(offset, limit)[1]);
                else if (limit > 0 && offset > 0) {
                    array.add(useDialect().getLimitValue(offset, limit)[1]);
                    array.add(useDialect().getLimitValue(offset, limit)[0]);
                }
            }
            else
                executeSQL = sql.toString();

            if (!array.isEmpty()) params = array.toArray();

            String _sql = executeSQL != null ? executeSQL : sql.toString();
            return (List<Map<String, Object>>) executeQuery(_sql, params);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public int countAll() {
        StringBuffer executeSQL = new StringBuffer();
        if (countSQL != null)
            executeSQL.append(countSQL);
        else {
            //			executeSQL.append(sql.substring(sql.lastIndexOf("from")));
            executeSQL.insert(0, "select count(1) as cnt ").append(" from (").append(sql.toString()).append(")");
        }
        if (show_sql) System.out.println(executeSQL);
        List<Map<String, Object>> rs = (List<Map<String, Object>>) executeQuery(executeSQL.toString(), this.params.toArray());
        if (!rs.isEmpty()) {
            Object cnt = rs.get(0).values().toArray()[0];
            try {
                return Integer.parseInt("" + cnt);
            } catch (NumberFormatException e) {}
        }
        return 0;
    }

    public int executeUpdate() {
        return executeUpdate(params.toArray());
    }
    public int executeUpdate(Object...params) {
        return ExecuteUpdate(sql.toString(), params);
    }

    public static <T> JdbcQuery createQuery() {
        return new JdbcQuery();
    }

    public String toString() {
        return executeSQL == null ? sql.toString() : executeSQL;
    }

    public static int ExecuteUpdate(String sql, Object...params) {
        try {
            return JdbcTemplate.New().executeUpdate(sql, params);
        }
        catch (Throwable ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Dialect useDialect() {
        String dbname = DB.getDBType();
        if (MySQLDialect.dbname.equals(dbname))
            return MySQLDialect.dialect;
        else
            return OracleDialect.dialect;
    }

	/* 数据库方言定义，引用Hibernate3的实现 */
    public interface Dialect {
        public boolean supportsLimit();
        public boolean supportsLimitOffset();
        public String getLimitString(String sql, boolean hasOffset);
        public String getLimitString(String querySelect, int offset, int limit);
        public int[] getLimitValue(int offset, int limit);
    }

    public static class OracleDialect implements Dialect {

        public static final String dbname = "oracle";

        public static OracleDialect dialect = new OracleDialect();

        private OracleDialect() {}

        public boolean supportsLimit() {
            return true;
        }

        public boolean supportsLimitOffset() {
            return true;
        }

        public String getLimitString(String sql, boolean hasOffset) {

            sql = sql.trim();
            boolean isForUpdate = false;
            if ( sql.toLowerCase().endsWith(" for update") ) {
                sql = sql.substring( 0, sql.length()-11 );
                isForUpdate = true;
            }

            StringBuilder pagingSelect = new StringBuilder( sql.length()+100 );
            if (hasOffset) {
                pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
            }
            else {
                pagingSelect.append("select * from ( ");
            }
            pagingSelect.append(sql);
            if (hasOffset) {
                pagingSelect.append(" ) row_ where rownum <= ?) where rownum_ > ?");
            }
            else {
                pagingSelect.append(" ) where rownum <= ?");
            }

            if ( isForUpdate ) {
                pagingSelect.append( " for update" );
            }

            return pagingSelect.toString();
        }

        public String getLimitString(String querySelect, int offset, int limit) {
            throw new UnsupportedOperationException( "Use \"String getLimitString(String sql, boolean hasOffset)\"" );
        }

        public int[] getLimitValue(int offset, int limit) {
            int[] value = new int[2];
            value[0] = offset;
            value[1] = limit + offset;
            return value;
        }

    }

    public static class MySQLDialect implements Dialect {

        public static final String dbname = "mysql";

        public static MySQLDialect dialect = new MySQLDialect();

        private MySQLDialect() {}

        public boolean supportsLimit() {
            return true;
        }

        public boolean supportsLimitOffset() {
            return true;
        }

        public String getLimitString(String sql, boolean hasOffset) {
            return new StringBuffer( sql.length()+20 )
                    .append(sql)
                    .append( hasOffset ? " limit ?, ?" : " limit ?")
                    .toString();
        }

        public String getLimitString(String querySelect, int offset, int limit) {
            throw new UnsupportedOperationException( "Use \"String getLimitString(String sql, boolean hasOffset)\"" );
        }

        public int[] getLimitValue(int offset, int limit) {
            int[] value = new int[2];
            value[0] = offset;
            value[1] = limit;
            return value;
        }
    }

    public static void transaction(Process p) {
        transaction(p, false);
    }
    public static void transaction(Process p, boolean autoCommit) {
        Connection conn = null;
        try {
            conn = DB.getConnection();
            conn.setAutoCommit(autoCommit);
            p.exec();
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            DB.disconnect();
        }
    }

    public static interface Process {
        void exec();
    }

    public static void main(String[] args) {
        JdbcQuery q = JdbcQuery.createQuery().select().concat("b.* from tms_post_line a, tms_post_station_seq b where a.line_id=b.line_id and");
        q.and(
                q.criteria("b.station_id").greaterThan("35000000"),
                q.criteria("a.begin_agency_code").greaterThanOrEq("35022100"),
                q.criteria().and(
                        q.criteria("a.end_agency_code").greaterThan("123"),
                        q.criteria("a.end_agency_code").like("321%"),
                        q.criteria("a.end_agency_code").in("321", "322", "323")
                )
        ).limit(10).offset(10).countAll();
        System.out.println(q.sql);
        System.out.println(q.params);
    }

}
