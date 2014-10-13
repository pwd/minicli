package com.jedou.common.cli.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.jedou.common.cli.util.ConfigUtil;

/**
 * Created by tiankai on 14-8-15.
 */
public class DB {
    static ThreadLocal<Connection> threadContext = new ThreadLocal<Connection>();
    static Connection getConnection() throws SQLException {
        Connection conn = threadContext.get();
        if (conn == null) {
            connect();
            conn = threadContext.get();
        }
        return conn;
    }
    static String getDBType() {
        return ConfigUtil.getProperty("jdbc.dbtype");
    };
    static void connect() throws SQLException {
        String driver = ConfigUtil.getProperty("jdbc.driverClass");
        String url = ConfigUtil.getProperty("jdbc.url");
        String user = ConfigUtil.getProperty("jdbc.user");
        String password = ConfigUtil.getProperty("jdbc.password");
        try {
            Class.forName(driver);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Connection conn = DriverManager.getConnection(url, user, password);
        threadContext.set(conn);
    }
    static void disconnect() {
        Connection conn = threadContext.get();
        if (conn != null) {
            try {
                conn.commit();
                conn.close();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        threadContext.remove();
    }
}
