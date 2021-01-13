package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class BaseXaSqlConnection extends AbstractXaSqlConnection {
    protected final ConcurrentHashMap<String, SqlConnection> map = new ConcurrentHashMap<>();
    protected final IdentityHashMap<SqlConnection, State> connectionState = new IdentityHashMap<>();
    protected final MySQLManager mySQLManager;
    protected String xid;
    final static AtomicLong ID = new AtomicLong();

    public BaseXaSqlConnection(MySQLManager mySQLManager, XaLog xaLog) {
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
                    connectionState.put(connection, State.XA_INIT);
                    Future<RowSet<Row>> execute = connection.query(String.format(XA_START, xid)).execute();
                    return execute.map(r -> {
                        connectionState.put(connection, State.XA_START);
                        return connection;
                    });
                });
            }
        }
        return mySQLManager.getConnection(targetName);
    }

    public void rollback(Handler<AsyncResult<Future>> handler) {
        executeAll(new Function<SqlConnection, Future>() {
            @Override
            public Future apply(SqlConnection c) {
                Future future = Future.succeededFuture();
                switch (connectionState.get(c)) {
                    case XA_INIT:
                        return future;
                    case XA_START:
                        future = future.compose(unused -> {
                            return c.query(String.format(XA_END, xid)).execute()
                                    .map(u -> connectionState.put(c, State.XA_END));
                        });
                    case XA_END:
                        future = future.compose(unuse -> c.query(String.format(XA_PREPARE, xid)).execute())
                                .map(u -> connectionState.put(c, State.XA_PREPARE));
                        ;
                    case XA_PREPARE:
                        future = future.compose(unuse -> c.query(String.format(XA_ROLLBACK, xid)).execute())
                                .map(u -> connectionState.put(c, State.XA_INIT));
                }
                return future;
            }
        })
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
        commitXa(() -> Future.succeededFuture(), handler);
    }

    public void commitXa(Supplier<Future> beforeCommit, Handler<AsyncResult<Future>> handler) {
        CompositeFuture xaEnd = executeAll(new Function<SqlConnection, Future>() {
            @Override
            public Future apply(SqlConnection connection) {
                Future future = Future.succeededFuture();
                switch (connectionState.get(connection)) {
                    case XA_INIT:
                        future = future
                                .compose(unuse -> connection.query(String.format(XA_START, xid)).execute())
                                .map(u -> connectionState.put(connection, State.XA_START));
                    case XA_START:
                        future = future
                                .compose(unuse -> connection.query(String.format(XA_END, xid)).execute())
                                .map(u -> connectionState.put(connection, State.XA_END));
                    case XA_END:
                    default:
                }
                return future;
            }

        });
        xaEnd.onFailure(event14 -> handler.handle(Future.failedFuture(event14)));
        xaEnd.onSuccess(event -> {
            executeAll(connection -> {
                if (connectionState.get(connection) != State.XA_PREPARE) {
                    return connection.query(String.format(XA_PREPARE, xid)).execute().map(c -> {
                        connectionState.put(connection, State.XA_PREPARE);
                        return connection;
                    });
                }
                return Future.succeededFuture();
            })
                    .onFailure(event13 -> {
                        //客户端触发回滚
                        handler.handle(Future.failedFuture(event13));
                    })
                    .onSuccess(event12 -> {

                        Future future = beforeCommit.get();
                        future.onSuccess(event16 -> executeAll(connection -> {
                            return connection.query(String.format(XA_COMMIT, xid)).execute()
                                    .map(c -> {
                                        connectionState.put(connection, State.XA_INIT);
                                        return connection;
                                    });
                        })
                                .onFailure(event15 -> {
                                    inTranscation = false;
                                    //记录日志,补偿
                                    clearConnections(event2 -> handler.handle(((AsyncResult) event15)));
                                })
                                .onSuccess(event1 -> {
                                    inTranscation = false;
                                    clearConnections(event2 -> handler.handle(((AsyncResult) event1)));
                                }));
                        future.onFailure(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
                                //客户端触发回滚
                                handler.handle(Future.failedFuture(event));
                            }
                        });

                    });
        });
    }

    public CompositeFuture executeAll(Function<SqlConnection, Future> connectionFutureFunction) {
        if (map.isEmpty()) {
            return CompositeFuture.any(Future.succeededFuture(), Future.succeededFuture());
        }
        List<Future> futures = map.values().stream().map(connectionFutureFunction).collect(Collectors.toList());
        return CompositeFuture.all(futures);
    }

    public void close(Handler<AsyncResult<Future>> handler) {
        if (inTranscation) {
            rollback((Handler<AsyncResult<Future>>) handler);
        } else {
          clearConnections(event -> handler.handle(Future.succeededFuture()));
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
        inTranscation = false;
        map.clear();
        connectionState.clear();
        handler.handle((Future) Future.succeededFuture());
    }
}

