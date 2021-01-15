package cn.mycat.vertx.xa;

import cn.mycat.vertx.xa.log.*;
import io.vertx.core.*;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.SqlConnection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class XaLogImpl implements XaLog {
    private final String name;
    private final Repository xaRepository;
    private final AtomicLong xaIdSeq = new AtomicLong();
    private final MySQLManager mySQLManager;
    private final static Logger LOGGER = LoggerFactory.getLogger(XaLogImpl.class);

    public XaLogImpl(Repository xaRepository, MySQLManager mySQLManager) {
        this("x", xaRepository, '.', mySQLManager);
    }

    public XaLogImpl(String name, Repository xaRepository, char sep, MySQLManager mySQLManager) {
        this.mySQLManager = mySQLManager;
        this.name = name + sep;
        this.xaRepository = xaRepository;

    }

    public static XaLog createDemoRepository(MySQLManager mySQLManager) {
        return new XaLogImpl(new MemoryRepositoryImpl()
                , mySQLManager);
    }


    private Future<Object> recoverConnection(ImmutableCoordinatorLog entry) {
        return Future.future(res -> {
            switch (entry.computeState()) {
                case XA_INITED:
                case XA_STARTED:
                case XA_ENDED: {
                    rollback(res, entry);
                    return;
                }
                case XA_PREPAREED: {
                    commit(res, entry);
                }

            }
        });
    }

    private void commit(Promise<Object> res, ImmutableCoordinatorLog entry) {
        for (ImmutableParticipantLog participant : entry.getParticipants()) {
            mySQLManager.getConnection(participant.getTarget())
                    .onComplete(event -> {
                        if (event.succeeded()) {
                            SqlConnection sqlConnection = event.result();
                            Future<SqlConnection> future = Future.succeededFuture(event.result());
                            switch (participant.getState()) {
                                case XA_PREPAREED:
                                    future = future
                                            .compose(connection -> connection.query(
                                                    String.format(XaSqlConnection.XA_COMMIT, entry.getXid())

                                            ).execute().map(connection));
                                    break;
                                case XA_COMMITED:
                                case XA_ROLLBACKED:
                            }
                            future.onSuccess(event1 -> {
                                sqlConnection.close();
                                res.complete();
                            });
                            future.onFailure(event12 -> {
                                sqlConnection.close();
                                res.fail(event12);
                            });
                        }
                    });

        }
    }

    private void rollback(Promise<Object> res, ImmutableCoordinatorLog entry) {
        for (ImmutableParticipantLog participant : entry.getParticipants()) {
            mySQLManager.getConnection(participant.getTarget())
                    .onComplete(event -> {
                        if (event.succeeded()) {
                            SqlConnection sqlConnection = event.result();
                            Future<SqlConnection> future = Future.succeededFuture(event.result());
                            switch (participant.getState()) {
                                case XA_INITED:
                                    break;
                                case XA_STARTED:
                                    future = future
                                            .compose(connection -> connection.query(
                                                    String.format(XaSqlConnection.XA_END, entry.getXid())

                                            ).execute().map(connection));
                                case XA_ENDED:
                                    future = future
                                            .compose(connection -> connection.query(
                                                    String.format(XaSqlConnection.XA_PREPARE, entry.getXid())

                                            ).execute().map(connection));
                                case XA_PREPAREED:
                                    future = future
                                            .compose(connection -> connection.query(
                                                    String.format(XaSqlConnection.XA_ROLLBACK, entry.getXid())

                                            ).execute().map(connection));
                                    break;
                                case XA_COMMITED:
                                case XA_ROLLBACKED:
                                    return;
                            }
                            future.onSuccess(event1 -> {
                                sqlConnection.close();
                                res.complete();
                            });
                            future.onFailure(event12 -> {
                                sqlConnection.close();
                                res.fail(event12);
                            });
                        }
                    });

        }
    }

    private void performXARecoveryLog(Handler<AsyncResult<XaLog>> handler) {
        long id = 0;
        char seq = name.charAt(name.length() - 1);
        Collection<ImmutableCoordinatorLog> entries = xaRepository.getAllCoordinatorLogEntries(true);
        for (ImmutableCoordinatorLog entry : entries) {
            String text = entry.getXid();
            xaRepository.put(text, entry);
            id = Math.max(id, Long.parseLong(text.substring(text.lastIndexOf(seq))));
        }
        xaIdSeq.set(id);

        List<Future> futures = new ArrayList<>();
        for (ImmutableCoordinatorLog entry : entries) {
            switch (entry.computeState()) {
                case XA_INITED:
                case XA_STARTED:
                case XA_ENDED:
                case XA_PREPAREED: {
                    futures.add(recoverConnection(entry));
                    break;
                }
                case XA_COMMITED:
                case XA_ROLLBACKED:
                    break;
                default:
            }
        }
        CompositeFuture.all(futures).onComplete(event -> {
            if (event.succeeded()) {
                handler.handle((AsyncResult) event);
                return;
            }
            CompositeFuture result = event.result();
            int size = result.size();
            for (int i = 0; i < size; i++) {
                Throwable cause = result.cause(i);
                System.out.println(cause);
            }
            handler.handle((AsyncResult) event);
            return;
        });
    }


    @Override
    public String nextXid() {
        long seq = xaIdSeq.getAndUpdate(operand -> {
            if (operand < 0) {
                return 0;
            }
            return ++operand;
        });
        return name + seq;
    }

    @Override
    public long getTimeout() {
        return xaRepository.getTimeout();
    }

    @Override
    public void log(String xid, ImmutableParticipantLog[] participantLogs) {
        if (xid == null) return;
        synchronized (xaRepository) {
            ImmutableCoordinatorLog immutableCoordinatorLog = new ImmutableCoordinatorLog(xid, participantLogs);
            xaRepository.put(xid,immutableCoordinatorLog );
        }
    }

    @Override
    public void log(String xid, String target, State state) {
        if (xid == null) return;
        synchronized (xaRepository) {
            ImmutableCoordinatorLog coordinatorLog = xaRepository.get(xid);
            if (coordinatorLog == null) {
                log(xid, new ImmutableParticipantLog[]{new ImmutableParticipantLog(xid,
                        getExpires(), state)});
            } else {
                ImmutableParticipantLog[] logs = coordinatorLog.replace(target, state, getExpires());
                if (state == State.XA_COMMITED){
                    log(xid,logs);
                }else {
                    log(xid,logs);
                }
            }
        }
    }

    @Override
    public void logTheImportantCommited(String xid, String target) {

    }

    @Override
    public void logRollback(String xid, boolean succeed) {
        if (xid == null) return;
        //only log
        if (!checkState(xid, succeed, State.XA_ROLLBACKED)) {
            LOGGER.error("check logRollback xid:" + xid + " error");
        }else if (succeed){
            synchronized (xaRepository){
                xaRepository.remove(xid);
            }
        }
    }

    @Override
    public void logPrepare(String xid, boolean succeed) {
        if (xid == null) return;
        //only log
        if (!checkState(xid, succeed, State.XA_PREPAREED)) {
            LOGGER.error("check logPrepare xid:" + xid + " error");
        }
    }

    private boolean checkState(String xid, boolean succeed, State state) {
        boolean s;
        ImmutableCoordinatorLog coordinatorLog = xaRepository.get(xid);

        if (coordinatorLog != null) {
            State recordState = coordinatorLog.computeState();
            s = (succeed == (recordState == state)) || (succeed && recordState == State.XA_COMMITED);
        } else {
            s = false;
        }
        return s;
    }

    @Override
    public void logCommit(String xid, boolean succeed) {
        if (xid == null) return;
        //only log
        if (!checkState(xid, succeed, State.XA_COMMITED)) {
            LOGGER.error("check logCommit xid:" + xid + " error");
        }else if (succeed){
            synchronized (xaRepository){
                xaRepository.remove(xid);
            }
        }
    }

    @Override
    public void logLocalCommitOnBeforeXaCommit(String xid, boolean succeed) {
        if (xid == null) return;
        //only log
        if (!checkState(xid, succeed, State.XA_PREPAREED)) {
            LOGGER.error("check logLocalCommitOnBeforeXaCommit xid:" + xid + " error");
        }
    }

    @Override
    public void beginXa(String xid) {
        if (xid == null) return;
        //only log
    }

    @Override
    public long getExpires() {
        return getTimeout() + System.currentTimeMillis();
    }
}
