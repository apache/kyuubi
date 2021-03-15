package org.apache.kyuubi.web.dao;

import org.apache.kyuubi.web.conf.DataSourceConfig;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class BaseDao {

    private DataSource ds = DataSourceConfig.load();
    private QueryRunner runner = new QueryRunner(ds);

    public BaseDao() {
    }

    protected Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Error to get connection", e);
        }
    }

    protected void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("Error to close connection", e);
        }
    }

    protected <T> T query(Function<Connection, T> fun) {
        Connection connection = getConnection();
        try {
            return fun.apply(connection);
        } finally {
            closeConnection(connection);
        }
    }

    protected <T> List<T> query(String sql, Class<T> clz, Object... params) {
        BeanListHandler<T> h = new BeanListHandler<T>(clz);
        try {
            return runner.query(sql, h, params);
        } catch (SQLException e) {
            throw new RuntimeException("Error to exec sql: " + sql, e);
        }
    }

    protected List<Map<String, Object>> queryMapList(String sql, Object... params) {
        MapListHandler mapListHandler = new MapListHandler();
        try {
            return runner.query(sql, mapListHandler, params);
        } catch (SQLException e) {
            throw new RuntimeException("Error to exec sql: " + sql, e);
        }
    }

    protected <T> T queryOne(String sql, Class<T> clz, Object... params) {
        ResultSetHandler<T> h = new BeanHandler<T>(clz);
        try {
            return runner.query(sql, h, params);
        } catch (SQLException e) {
            throw new RuntimeException("Error to exec sql: " + sql, e);
        }
    }

    protected long count(String sql, Object... params) {
        ScalarHandler<Long> scalarHandler = new ScalarHandler<Long>();
        try {
            Long query = runner.query(sql, scalarHandler, params);
            return query;
        } catch (SQLException e) {
            throw new RuntimeException("Error to exec sql: " + sql, e);
        }
    }

    protected void exec(Consumer<Connection> con) {
        Connection connection = getConnection();
        try {
            con.accept(connection);
        } finally {
            closeConnection(connection);
        }
    }

    protected void exec(String sql, Object... obj) {
        Connection connection = getConnection();
        try {
            runner.execute(sql, obj);
        } catch (SQLException e) {
            throw new RuntimeException("Error to exec sql: " + sql, e);
        } finally {
            closeConnection(connection);
        }
    }


    public static Object[] nonBlankParams(Object... params) {
        if (params == null) {
            return null;
        }
        List<Object> list = new ArrayList<>();
        for (Object obj : params) {
            if (obj == null) {
                continue;
            }
            if (obj instanceof String && StringUtils.isBlank((String) obj)) {
                continue;
            }
            list.add(obj);
        }
        return list.toArray(new Object[0]);
    }

    public static String likeParam(Object param) {
        if (param == null) {
            return null;
        }
        if (param instanceof String && StringUtils.isBlank((CharSequence) param)) {
            return null;
        }
        return "%" + param + "%";
    }

    public static String dateToStr(Date date) {
        if (date == null) {
            return null;
        }
        return DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");
    }
}
