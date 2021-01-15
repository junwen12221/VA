package cn.mycat.vertx.xa;

public enum State {
    XA_INITED,
    XA_STARTED,
    XA_ENDED,
    XA_PREPAREED,

    XA_COMMITED,
    XA_ROLLBACKED
    ;
}