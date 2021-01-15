package cn.mycat.vertx.xa;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;


interface MySQLManager {

    Future<SqlConnection> getConnection(String targetName);
    Future<SqlConnection> getConnection(String host,int port);
    public String getDatasourceName(String host, int port);
    void close(Handler<Future> handler);
    Vertx getVertx();
}