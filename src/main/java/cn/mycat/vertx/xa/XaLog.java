package cn.mycat.vertx.xa;

public interface XaLog {
   void log(String cmd,String xid);
    void log(String cmd,String xid,Throwable throwable);
}
