package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public abstract class AbstractXaSqlConnection implements XaSqlConnection {
    protected boolean autocommit;
    protected boolean inTranscation = false;
    protected  final XaLog log;

    public AbstractXaSqlConnection(XaLog xaLog) {
        this.log = xaLog;
    }

    @Override
    public boolean isAutocommit() {
        return autocommit;
    }

    @Override
    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    @Override
    public void openStatementState(Handler<AsyncResult<Void>> handler) {
        if (!autocommit) {
            if (!isInTranscation()) {
                begin(handler);
                return;
            }
        }
        handler.handle(Future.succeededFuture());
    }

    @Override
    public boolean isInTranscation() {
        return inTranscation;
    }

}
