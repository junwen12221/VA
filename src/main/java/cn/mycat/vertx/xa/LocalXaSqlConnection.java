package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

public class LocalXaSqlConnection extends OnePhaseXaSqlConnection {
    volatile SqlConnection localSqlConnection = null;
    volatile String targetName;

    public LocalXaSqlConnection(XaLog xaLog, MySQLManager mySQLManager) {
        super(xaLog, mySQLManager);
    }

    @Override
    public void commit(Handler<AsyncResult<Future>> handler) {
        if (targetName == null && localSqlConnection == null && map.isEmpty()) {
            handler.handle(Future.succeededFuture());
            return;
        }
        if (targetName != null && inTranscation && localSqlConnection != null) {
            Future<RowSet<Row>> execute = localSqlConnection.query("commit;").execute();
            execute.onSuccess(event -> LocalXaSqlConnection.super.commit(handler));
            execute.onFailure((Handler) handler);//用户触发回滚
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        if (inTranscation) {
            if (targetName == null && localSqlConnection == null) {
                LocalXaSqlConnection.this.targetName = targetName;
                Future<SqlConnection> sqlConnectionFuture = mySQLManager.getConnection(targetName);
                return sqlConnectionFuture.map(sqlConnection -> {
                    LocalXaSqlConnection.this.localSqlConnection = sqlConnection;
                    return sqlConnection;
                }).compose(sqlConnection -> sqlConnection.begin().map(sqlConnection));
            }
            if (LocalXaSqlConnection.this.targetName.equals(targetName)) {
                return Future.succeededFuture(localSqlConnection);
            }
            return super.getConnection(targetName);
        }
        return mySQLManager.getConnection(targetName);
    }

    @Override
    public void rollback(Handler<AsyncResult<Future>> handler) {
        if (targetName == null && localSqlConnection == null && map.isEmpty()) {
            inTranscation = false;
            handler.handle(Future.succeededFuture());
            return;
        }
        localSqlConnection.query("rollback;").execute()
                .onComplete(event -> {
                    if (event.failed()) {
                        //记录日志
                    }
                    LocalXaSqlConnection.super.rollback(handler);
                });
    }

    @Override
    public void closeStatementState(Handler<AsyncResult<Void>> handler) {
        if (!isInTranscation()) {
            targetName = null;
            SqlConnection localSqlConnection = this.localSqlConnection;
            this.localSqlConnection = null;
            localSqlConnection.close(event -> LocalXaSqlConnection.super.closeStatementState(handler));
            return;
        }
        super.closeStatementState(handler);
    }
}
