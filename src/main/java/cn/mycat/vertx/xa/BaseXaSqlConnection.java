package cn.mycat.vertx.xa;

import cn.mycat.vertx.xa.log.ImmutableParticipantLog;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class BaseXaSqlConnection extends AbstractXaSqlConnection {
    protected final ConcurrentHashMap<String, SqlConnection> map = new ConcurrentHashMap<>();
    protected final Map<SqlConnection, State> connectionState = Collections.synchronizedMap(new IdentityHashMap<>());
    protected final MySQLManager mySQLManager;
    protected String xid;

    private String getTarget(SqlConnection connection) {
        return map.entrySet().stream().filter(p -> p.getValue() == connection).map(e -> e.getKey())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown connection " + connection));
    }

    public BaseXaSqlConnection(MySQLManager mySQLManager, XaLog xaLog) {
        super(xaLog);
        this.mySQLManager = mySQLManager;
    }

    public BaseXaSqlConnection(String xid, MySQLManager mySQLManager, XaLog xaLog) {
        super(xaLog);
        this.xid = xid;
        this.mySQLManager = mySQLManager;
    }


    public void begin(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            handler.handle(Future.failedFuture(new IllegalArgumentException("occur Nested transaction")));
            return;
        }
        inTranscation = true;
        xid = log.nextXid();
        log.beginXa(xid);
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
                    changeTo(connection, State.XA_INITED);
                    Future<RowSet<Row>> execute = connection.query(String.format(XA_START, xid)).execute();
                    return execute.map(r -> changeTo(connection, State.XA_STARTED));
                });
            }
        }
        return mySQLManager.getConnection(targetName);
    }

    public void rollback(Handler<AsyncResult<Void>> handler) {
        logParticipants();
        Function<SqlConnection, Future<Object>> function = new Function<SqlConnection, Future<Object>>() {
            @Override
            public Future apply(SqlConnection c) {
                Future future = Future.succeededFuture();
                switch (connectionState.get(c)) {
                    case XA_INITED:
                        return future;
                    case XA_STARTED:
                        future = future.compose(unused -> {
                            return c.query(String.format(XA_END, xid)).execute()
                                    .map(u -> changeTo(c, State.XA_ENDED));
                        });
                    case XA_ENDED:
                        future = future.compose(unuse -> c.query(String.format(XA_PREPARE, xid)).execute())
                                .map(u -> changeTo(c, State.XA_PREPAREED));
                        ;
                    case XA_PREPAREED:
                        future = future.compose(unuse -> c.query(String.format(XA_ROLLBACK, xid)).execute())
                                .map(u -> changeTo(c, State.XA_ROLLBACKED));
                }
                return future;
            }
        };
        executeAll((Function)function)
                .onComplete(event -> {
                    log.logRollback(xid, event.succeeded());
                    if (event.succeeded()){
                        inTranscation = false;
                        clearConnections((Handler) handler);
                    }else {
                        retryRollback(handler,function);
                    }
                });
    }

    private void retryRollback(Handler<AsyncResult<Void>> handler, Function<SqlConnection, Future<Object>> function) {
        List<Future<Object>> collect = computePrepareRollbackTargets().stream().map(c -> mySQLManager.getConnection(c).flatMap(function)).collect(Collectors.toList());
        CompositeFuture.all((List)collect)
                .onComplete(event -> {
                    log.logRollback(xid, event.succeeded());
                    if (event.failed()){
                        retryRollback(handler, function);
                        return;
                    }
                    inTranscation = false;
                    clearConnections(handler);
                });
    }

    private void logParticipants() {
        ImmutableParticipantLog[] participantLogs = new ImmutableParticipantLog[map.size()];
        int index = 0;
        for (Map.Entry<String, SqlConnection> e : map.entrySet()) {
            participantLogs[index] = new ImmutableParticipantLog(e.getKey(),
                    log.getExpires(),
                    connectionState.get(e.getValue()));
            index++;
        }
        log.log(xid, participantLogs);
    }

    protected SqlConnection changeTo(SqlConnection c, State state) {
        connectionState.put(c, state);
        log.log(xid, getTarget(c), state);
        return c;
    }

    protected void changeTo(String c, State state) {
        connectionState.put(map.get(c), state);
        log.log(xid, c, state);
    }

    public void commit(Handler<AsyncResult<Void>> handler) {
        commitXa(() -> Future.succeededFuture(), handler);
    }

    public void commitXa(Supplier<Future> beforeCommit, Handler<AsyncResult<Void>> handler) {
        logParticipants();
        CompositeFuture xaEnd = executeAll(new Function<SqlConnection, Future>() {
            @Override
            public Future apply(SqlConnection connection) {
                Future future = Future.succeededFuture();
                switch (connectionState.get(connection)) {
                    case XA_INITED:
                        future = future
                                .compose(unuse -> connection.query(String.format(XA_START, xid)).execute())
                                .map(u -> changeTo(connection, State.XA_STARTED));
                    case XA_STARTED:
                        future = future
                                .compose(unuse -> connection.query(String.format(XA_END, xid)).execute())
                                .map(u -> changeTo(connection, State.XA_ENDED));
                    case XA_ENDED:
                    default:
                }
                return future;
            }

        });
        xaEnd.onFailure(event14 -> handler.handle(Future.failedFuture(event14)));
        xaEnd.onSuccess(event -> {
            executeAll(connection -> {
                if (connectionState.get(connection) != State.XA_PREPAREED) {
                    return connection.query(String.format(XA_PREPARE, xid)).execute()
                            .map(c -> changeTo(connection, State.XA_PREPAREED));
                }
                return Future.succeededFuture();
            })
                    .onFailure(event13 -> {
                        log.logPrepare(xid, false);
                        //客户端触发回滚
                        handler.handle(Future.failedFuture(event13));
                    })
                    .onSuccess(event12 -> {
                        log.logPrepare(xid, true);
                        Future future = beforeCommit.get();
                        future.onSuccess(event16 -> {
                            if (future != Future.succeededFuture()) {
                                log.logLocalCommitOnBeforeXaCommit(xid, true);
                            }
                            executeAll(connection -> {
                                return connection.query(String.format(XA_COMMIT, xid)).execute()
                                        .map(c -> changeTo(connection, State.XA_COMMITED));
                            })
                                    .onFailure(event15 -> {
                                        log.logCommit(xid, false);
                                        //retry
                                        retryCommit(handler);
                                    })
                                    .onSuccess(event1 -> {
                                        inTranscation = false;
                                        log.logCommit(xid, true);
                                        clearConnections(event2 -> handler.handle(((AsyncResult) event1)));
                                    });
                        });
                        future.onFailure(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
                                if (future != Future.succeededFuture()) {
                                    log.logLocalCommitOnBeforeXaCommit(xid, false);
                                }
                                //客户端触发回滚
                                handler.handle(Future.failedFuture(event));
                            }
                        });

                    });
        });
    }

    private void retryCommit(Handler<AsyncResult<Void>> handler) {
        CompositeFuture all = CompositeFuture.all(computePrepareCommittedTargets().stream()
                .map(s -> mySQLManager.getConnection(s)
                        .compose(c -> {
                            return c.query(String.format(XA_COMMIT, xid))
                                    .execute().compose(rows -> {
                                        changeTo(s, State.XA_COMMITED);
                                        c.close();
                                        return Future.succeededFuture();
                                    }, throwable -> {
                                        c.close();
                                        return Future.failedFuture(throwable);
                                    });
                        })).collect(Collectors.toList()));
        all.onSuccess(event -> {
            inTranscation = false;
            log.logCommit(xid,true);
            clearConnections(event2 -> handler.handle(Future.succeededFuture()));
        });
        all.onFailure(event -> retryCommit(handler));
    }

    private List<String> computePrepareCommittedTargets() {
        List<String> collect = connectionState.entrySet().stream()
                .filter(i -> i.getValue() != State.XA_COMMITED)
                .map(i -> i.getKey())
                .map(k -> getTarget(k)).collect(Collectors.toList());
        return collect;
    }
    private List<String> computePrepareRollbackTargets() {
        List<String> collect = connectionState.entrySet().stream()
                .filter(i -> i.getValue() != State.XA_ROLLBACKED)
                .map(i -> i.getKey())
                .map(k -> getTarget(k)).collect(Collectors.toList());
        return collect;
    }
    public CompositeFuture executeAll(Function<SqlConnection, Future> connectionFutureFunction) {
        if (map.isEmpty()) {
            return CompositeFuture.any(Future.succeededFuture(), Future.succeededFuture());
        }
        List<Future> futures = map.values().stream().map(connectionFutureFunction).collect(Collectors.toList());
        return CompositeFuture.all(futures);
    }

    public void close(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            rollback((Handler<AsyncResult<Void>>) handler);
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

