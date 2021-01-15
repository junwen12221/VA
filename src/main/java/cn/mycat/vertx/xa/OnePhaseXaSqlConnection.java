package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.SqlConnection;

public class OnePhaseXaSqlConnection extends BaseXaSqlConnection {
    public OnePhaseXaSqlConnection(MySQLManager mySQLManager,XaLog xaLog) {
        super(mySQLManager,xaLog);
    }

    @Override
    public void commit(Handler<AsyncResult<Void>> handler) {
        if (map.size() == 1) {
            SqlConnection sqlConnection = map.values().iterator().next();
            CompositeFuture xaEnd = executeAll(connection ->
                    connection.query(String.format(XA_END, xid)).execute());
            xaEnd.onFailure(event14 -> {
                handler.handle(Future.failedFuture(event14));
            });
            xaEnd.onSuccess(event -> {
                changeTo(sqlConnection,State.XA_ENDED);
                executeAll(connection -> {
                    return connection.query(String.format(XA_COMMIT_ONE_PHASE, xid)).execute();
                }).onComplete(new Handler<AsyncResult<CompositeFuture>>() {
                    @Override
                    public void handle(AsyncResult<CompositeFuture> event) {
                        if (event.succeeded()){
                            changeTo(sqlConnection,State.XA_COMMITED);
                            inTranscation = false;
                        }
                        handler.handle((AsyncResult)event);
                    }
                });
            });
            }else{
                super.commit(handler);
            }
        }
    }
