package cn.mycat.vertx.xa;

import cn.mycat.vertx.xa.log.ImmutableParticipantLog;

public interface XaLog {
    void log(String xid, ImmutableParticipantLog[] participantLogs);

    void log(String xid, String target, State state);
    void logTheImportantCommited(String xid, String target);
    String nextXid();

    long getTimeout();

    void logRollback(String xid, boolean succeed);

    void logPrepare(String xid, boolean succeed);

    void logCommit(String xid, boolean succeed);

    void logLocalCommitOnBeforeXaCommit(String xid, boolean succeed);

    void beginXa(String xid);

    long getExpires();
}
