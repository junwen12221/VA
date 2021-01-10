package cn.mycat.vertx.xa;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.SqlConnection;


interface MySQLManager {

    Future<SqlConnection> getConnection(String targetName);

    void close(Handler<Future> handler);
}