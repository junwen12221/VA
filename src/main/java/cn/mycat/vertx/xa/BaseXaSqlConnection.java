package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;


public class BaseXaSqlConnection extends AbstractXaSqlConnection {
    protected final ConcurrentHashMap<String, SqlConnection> map = new ConcurrentHashMap<>();

    protected final MySQLManager mySQLManager;
    protected String xid;
    final static AtomicLong ID = new AtomicLong();

    public BaseXaSqlConnection(MySQLManager mySQLManager,XaLog xaLog) {
        super(xaLog);
        this.mySQLManager = mySQLManager;
    }


    public void begin(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            handler.handle(Future.failedFuture(new IllegalArgumentException("occur Nested transaction")));
            return;
        }
        inTranscation = true;
        xid = ID.getAndIncrement() + "";
        log.log(CMD_ALLOC_XID, xid);
        handler.handle(Future.succeededFuture());
    }


    public Future<SqlConnection> getConnection(String targetName) {
        if (inTranscation) {
            if (map.containsKey(targetName)) {
                return Future.succeededFuture(map.get(targetName));
            } else {
                Future<SqlConnection> sqlConnectionFuture = mySQLManager.getConnection(targetName);
                return sqlConnectionFuture.compose(connection -> {
                    map.put(targetName, connection);
                    Future<RowSet<Row>> execute = connection.query(String.format(XA_START, xid)).execute();
                    return execute.map(r -> connection);
                });
            }
        }
        return mySQLManager.getConnection(targetName);
    }

    public void rollback(Handler<AsyncResult<Future>> handler) {
        executeAll(c -> c.query(String.format(XA_ROLLBACK, xid)).execute())
                .onComplete(new Handler<AsyncResult<CompositeFuture>>() {
                    @Override
                    public void handle(AsyncResult<CompositeFuture> event) {
                        inTranscation = false;
                        event.succeeded();//记录日志
//.....
//...
                        clearConnections((Handler) handler);
                    }
                });
    }

    public void commit(Handler<AsyncResult<Future>> handler) {
        CompositeFuture xaEnd = executeAll(connection -> connection.query(String.format(XA_END, xid)).execute());
        xaEnd.onFailure(event14 -> handler.handle(Future.failedFuture(event14)));
        xaEnd.onSuccess(event -> {
            executeAll(connection -> {
                return connection.query(String.format(XA_PREPARE, xid)).execute();
            })
                    .onFailure(event13 -> {
                        //客户端触发回滚
                        handler.handle(Future.failedFuture(event13));
                    })
                    .onSuccess(event12 -> {
                        executeAll(connection -> {
                            return connection.query(String.format(XA_COMMIT, xid)).execute();
                        })
                                .onFailure(event15 -> {
                                    inTranscation = false;
                                    //记录日志,补偿
                                    clearConnections(event2 -> handler.handle(((AsyncResult) event15)));
                                })
                                .onSuccess(event1 -> {
                                    inTranscation = false;
                                    clearConnections(event2 -> handler.handle(((AsyncResult) event1)));
                                });
                    });
        });
    }

    public CompositeFuture executeAll(Function<SqlConnection, Future> connectionFutureFunction) {
        if (map.isEmpty()){
            return CompositeFuture.any(Future.succeededFuture(),Future.succeededFuture());
        }
        List<Future> futures = map.values().stream().map(connectionFutureFunction).collect(Collectors.toList());
        return CompositeFuture.all(futures);
    }

    public void close(Handler<AsyncResult<Future>> handler) {
        if (inTranscation) {
            inTranscation = false;
            rollback((Handler) handler);
        } else {
            inTranscation = false;
            CompositeFuture compositeFuture = executeAll(sqlConnection -> sqlConnection.close());
            compositeFuture.onComplete((Handler) handler);
        }
    }

    public void closeStatementState(Handler<AsyncResult<Void>> handler) {
        if (!inTranscation) {
            clearConnections(handler);
        }
    }

    private void clearConnections(Handler<AsyncResult<Void>> handler) {
        executeAll(c -> c.close()).onComplete(event -> {
            if (event.failed()) {
                //记录日志
            }
        });
        map.clear();
        handler.handle((Future) Future.succeededFuture());
    }
}

