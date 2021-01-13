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
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LocalSqlConnection extends AbstractXaSqlConnection {
    protected final ConcurrentHashMap<String, SqlConnection> map = new ConcurrentHashMap<>();

    protected final MySQLManager mySQLManager;
    protected String xid;
    final static AtomicLong ID = new AtomicLong();

    public LocalSqlConnection(MySQLManager mySQLManager, XaLog xaLog) {
        super(xaLog);
        this.mySQLManager = mySQLManager;
    }

    @Override
    public void begin(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            handler.handle(Future.failedFuture(new IllegalArgumentException("occur Nested transaction")));
            return;
        }
        inTranscation = true;
        xid = ID.getAndIncrement() + "";
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
                    Future<RowSet<Row>> execute = connection.query("begin").execute();
                    return execute.map(r -> connection);
                });
            }
        }
        return mySQLManager.getConnection(targetName);
    }

    @Override
    public void rollback(Handler<AsyncResult<Future>> handler) {
        List<Future> rollback = map.values().stream().map(c -> c.query("rollback").execute()).collect(Collectors.toList());
        CompositeFuture.all(rollback).onComplete(event -> {
            map.clear();
            inTranscation = false;
            //每一个记录日志
            handler.handle((AsyncResult) event);
        });
    }

    @Override
    public void commit(Handler<AsyncResult<Future>> handler) {
        List<Future> rollback = map.values().stream().map(c -> c.query("commit").execute()).collect(Collectors.toList());
        CompositeFuture.all(rollback).onComplete(event -> {
            map.clear();
            inTranscation = false;
            //每一个记录日志
            handler.handle((AsyncResult) event);
        });
    }

    @Override
    public void commitXa(Supplier<Future> beforeCommit, Handler<AsyncResult<Future>> handler) {
        beforeCommit.get().onComplete((Handler<AsyncResult>) event -> {
            if (event.succeeded()){
                commit(handler);
            }else {
                handler.handle(Future.failedFuture(event.cause()));
            }
        });
    }

    @Override
    public void close(Handler<AsyncResult<Future>> handler) {
        inTranscation = false;
        List<Future> rollback = map.values().stream().map(c -> c.close()).collect(Collectors.toList());
        CompositeFuture.all(rollback).onComplete(event -> {
            //每一个记录日志
        });
        handler.handle(Future.succeededFuture());
    }

    @Override
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

    public CompositeFuture executeAll(Function<SqlConnection, Future> connectionFutureFunction) {
        if (map.isEmpty()) {
            return CompositeFuture.any(Future.succeededFuture(), Future.succeededFuture());
        }
        List<Future> futures = map.values().stream().map(connectionFutureFunction).collect(Collectors.toList());
        return CompositeFuture.all(futures);
    }
}
