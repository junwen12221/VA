package cn.mycat.vertx.xa;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.util.JsonUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@ExtendWith(VertxExtension.class)
public class HappyPass {
    MySQLManagerImpl mySQLManager = new MySQLManagerImpl(Arrays.asList(demoConfig("ds1", 3307)
            , demoConfig("ds2", 3306)));
    XaLogImpl xaLog = new XaLogImpl();
    String DB1 = System.getProperty("db1", "jdbc:mysql://127.0.0.1:3306/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
    String DB2 = System.getProperty("db2", "jdbc:mysql://127.0.0.1:3307/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");


    public HappyPass() throws Exception {
        Connection mySQLConnection = getMySQLConnection(DB2);
        extracteInitSql(mySQLConnection);
        mySQLConnection.close();
        mySQLConnection = getMySQLConnection(DB1);
        extracteInitSql(mySQLConnection);
        mySQLConnection.close();
    }

    private void extracteInitSql(Connection mySQLConnection) throws SQLException {
        JdbcUtils.execute(mySQLConnection, "CREATE DATABASE IF NOT EXISTS db1;");
        JdbcUtils.execute(mySQLConnection, "CREATE TABLE IF NOT EXISTS db1.`travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
    }


    @Test
    public void begin(VertxTestContext testContext) {
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                Assertions.assertEquals(baseXaSqlConnection.isInTranscation(), true);
                baseXaSqlConnection.close();
                testContext.completeNow();
            }
        });
    }

    @Test
    public void beginCommit(VertxTestContext testContext) {
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.begin(event -> baseXaSqlConnection.commit(new Handler<AsyncResult<Future>>() {
            @Override
            public void handle(AsyncResult<Future> event) {
                Assertions.assertEquals(baseXaSqlConnection.isInTranscation(), false);
                baseXaSqlConnection.close();
                testContext.completeNow();
            }
        }));
    }

    @Test
    public void beginRollback(VertxTestContext testContext) {
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.begin(event -> baseXaSqlConnection.rollback(new Handler<AsyncResult<Future>>() {
            @Override
            public void handle(AsyncResult<Future> event) {
                Assertions.assertEquals(baseXaSqlConnection.isInTranscation(), false);
                baseXaSqlConnection.close();
                testContext.completeNow();
            }
        }));
    }

    @Test
    public void beginBegin(VertxTestContext testContext) {
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                baseXaSqlConnection.begin(event1 -> {
                    Assertions.assertTrue(event1.failed());
                    testContext.completeNow();
                });

            }
        });
    }

    @Test
    public void rollback(VertxTestContext testContext) {
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.rollback(new Handler<AsyncResult<Future>>() {
            @Override
            public void handle(AsyncResult<Future> event) {
                Assertions.assertTrue(event.succeeded());
                Assertions.assertFalse(baseXaSqlConnection.isInTranscation());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void commit(VertxTestContext testContext) {
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.commit(new Handler<AsyncResult<Future>>() {
            @Override
            public void handle(AsyncResult<Future> event) {
                Assertions.assertTrue(event.succeeded());
                Assertions.assertFalse(baseXaSqlConnection.isInTranscation());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void close(VertxTestContext testContext) {
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.close(new Handler<AsyncResult<Future>>() {
            @Override
            public void handle(AsyncResult<Future> event) {
                Assertions.assertTrue(event.succeeded());
                Assertions.assertFalse(baseXaSqlConnection.isInTranscation());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void closeInTranscation(VertxTestContext testContext) {
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                baseXaSqlConnection.close(new Handler<AsyncResult<Future>>() {
                    @Override
                    public void handle(AsyncResult<Future> event) {
                        Assertions.assertTrue(event.succeeded());
                        Assertions.assertFalse(baseXaSqlConnection.isInTranscation());
                        testContext.completeNow();
                    }
                });
            }
        });
    }

    @Test
    public void beginSingleTargetInsertCommit(VertxTestContext testContext) throws Exception {
        clearData();
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.begin(event -> {
            Assertions.assertTrue(event.succeeded());
            Future<SqlConnection> ds1 = baseXaSqlConnection.getConnection("ds1");
            ds1.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (1);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }).compose(connection -> {
                return connection.query("select id from db1.travelrecord").execute()
                        .compose(rows -> {
                            Assertions.assertEquals(1, rows.size());
                            return Future.succeededFuture(connection);
                        });
            }).onComplete(event13 -> {
                Assertions.assertTrue(event13.succeeded());
                baseXaSqlConnection.commit(event12 -> {
                    Assertions.assertTrue(event12.succeeded());
                    Assertions.assertFalse(baseXaSqlConnection.isInTranscation());
                    Future<SqlConnection> connectionFuture =
                            baseXaSqlConnection.getConnection("ds1");
                    connectionFuture
                            .compose(sqlConnection ->
                                    sqlConnection.query("select id from db1.travelrecord").execute())
                            .onComplete(event1 -> {
                                Assertions.assertTrue(event1.succeeded());
                                Assertions.assertEquals(1, event1.result().size());

                                testContext.completeNow();
                            });
                });
            });
        });
    }

    @Test
    public void beginDoubleTargetInsertCommit(VertxTestContext testContext) throws Exception {
        clearData();
        BaseXaSqlConnection baseXaSqlConnection = new BaseXaSqlConnection(xaLog, mySQLManager);
        baseXaSqlConnection.begin(event -> {
            Assertions.assertTrue(event.succeeded());
            Future<SqlConnection> ds1 = baseXaSqlConnection.getConnection("ds1");
            Future<SqlConnection> ds2 = baseXaSqlConnection.getConnection("ds2");

            CompositeFuture all = CompositeFuture.all(ds1.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (1);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }), ds2.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (2);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }));
            all.onComplete(event13 -> {
                Assertions.assertTrue(event13.succeeded());
                baseXaSqlConnection.commit(event12 -> {
                    Assertions.assertTrue(event12.succeeded());
                    Assertions.assertFalse(baseXaSqlConnection.isInTranscation());
                    Future<SqlConnection> connectionFuture =
                            baseXaSqlConnection.getConnection("ds2");
                    connectionFuture
                            .compose(sqlConnection ->
                                    sqlConnection.query("select id from db1.travelrecord").execute())
                            .onComplete(event1 -> {
                                Assertions.assertTrue(event1.succeeded());
                                Assertions.assertEquals(1, event1.result().size());

                                testContext.completeNow();
                            });
                });
            });
        });
    }

    private void clearData() throws SQLException {
        Connection mySQLConnection = getMySQLConnection(DB2);
        mySQLConnection.createStatement().execute("delete from db1.travelrecord");
        mySQLConnection.close();
        mySQLConnection = getMySQLConnection(DB1);
        mySQLConnection.createStatement().execute("delete from db1.travelrecord");
        mySQLConnection.close();
    }

    public SimpleConfig demoConfig(String name, int port) {
        SimpleConfig simpleConfig = new SimpleConfig(name, "127.0.0.1", port, "root", "123456", "mysql", 5);
        return simpleConfig;
    }

    private static Map<String, DruidDataSource> dsMap = new ConcurrentHashMap<>();


    Connection getMySQLConnection(String url) {
        try {
            return dsMap.computeIfAbsent(url, new Function<String, DruidDataSource>() {
                @Override
                public DruidDataSource apply(String url) {
                    Map<String, String> urlParameters = JsonUtil.urlSplit(url);
                    String username = urlParameters.getOrDefault("username", "root");
                    String password = urlParameters.getOrDefault("password", "123456");

                    DruidDataSource dataSource = new DruidDataSource();
                    dataSource.setUrl(url);
                    dataSource.setUsername(username);
                    dataSource.setPassword(password);
                    dataSource.setLoginTimeout(5);
                    dataSource.setCheckExecuteTime(true);
                    dataSource.setMaxWait(TimeUnit.SECONDS.toMillis(10));
                    return dataSource;
                }
            }).getConnection();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
