package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.SqlConnection;

public interface XaSqlConnection {
    public static String XA_START = "XA START '%s';";
    public static String XA_END = "XA END '%s';";
    public static String XA_COMMIT = "XA COMMIT '%s';";
    public static String XA_PREPARE = "XA PREPARE '%s';";
    public static String XA_ROLLBACK = "XA ROLLBACK '%s';";
    public static String XA_COMMIT_ONE_PHASE = "XA COMMIT '%s' ONE PHASE;";
    public static String XA_RECOVER = "XA RECOVER;";
    public static String CMD_ALLOC_XID = "ALLOC XID '%s'";

    enum State {
        XA_INIT,
        XA_START,
        XA_END,
        XA_PREPARE,
        ;
    }

    public void begin(Handler<AsyncResult<Void>> handler);


    public Future<SqlConnection> getConnection(String targetName);

    public void rollback(Handler<AsyncResult<Future>> handler);

    public void commit(Handler<AsyncResult<Future>> handler);

    public default void close() {
        close(event -> {

        });
    }

    public void close(Handler<AsyncResult<Future>> handler);

    public void openStatementState(Handler<AsyncResult<Void>> handler);

    public void closeStatementState(Handler<AsyncResult<Void>> handler);

    public void setAutocommit(boolean b);

    public boolean isAutocommit();

    public boolean isInTranscation();

}
