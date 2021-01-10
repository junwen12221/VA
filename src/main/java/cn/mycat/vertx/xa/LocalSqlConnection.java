package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.SqlConnection;

public class LocalSqlConnection extends AbstractXaSqlConnection{
    public LocalSqlConnection(XaLog xaLog) {
        super(xaLog);
    }

    @Override
    public void begin(Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        return null;
    }

    @Override
    public void rollback(Handler<AsyncResult<Future>> handler) {

    }

    @Override
    public void commit(Handler<AsyncResult<Future>> handler) {

    }

    @Override
    public void close(Handler<AsyncResult<Future>> handler) {

    }

    @Override
    public void closeStatementState(Handler<AsyncResult<Void>> handler) {

    }
}
