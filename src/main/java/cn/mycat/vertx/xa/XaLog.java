package cn.mycat.vertx.xa;

import cn.mycat.vertx.xa.log.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.log.ImmutableParticipantLog;

import java.io.Closeable;

public interface XaLog extends AutoCloseable , Closeable {
    void log(String xid, ImmutableParticipantLog[] participantLogs);

    void log(String xid, String target, State state);

    String nextXid();

    long getTimeout();

    void logRollback(String xid, boolean succeed);

    void logPrepare(String xid, boolean succeed);

    void logCommit(String xid, boolean succeed);

    void logCommitBeforeXaCommit(String xid);
    void logCancelLocalCommitBeforeXaCommit(String xid);
    void beginXa(String xid);

    long getExpires();

    long retryDelay();
}
