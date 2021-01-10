package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class OnePhaseXaSqlConnection extends BaseXaSqlConnection {
    public OnePhaseXaSqlConnection(XaLog xaLog,MySQLManager mySQLManager) {
        super(xaLog,mySQLManager);
    }

    @Override
    public void commit(Handler<AsyncResult<Future>> handler) {
        if (map.size() == 1) {
            CompositeFuture xaEnd = executeAll(connection -> connection.query(String.format(XA_END, xid)).execute());
            xaEnd.onFailure(event14 -> handler.handle(Future.failedFuture(event14)));
            xaEnd.onSuccess(event -> {
                executeAll(connection -> {
                    return connection.query(String.format(XA_COMMIT_ONE_PHASE, xid)).execute();
                }).onComplete((Handler)handler);
            });
            }else{
                super.commit(handler);
            }
        }
    }
