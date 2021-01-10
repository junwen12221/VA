package cn.mycat.vertx.xa;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.mysqlclient.MySQLAuthenticationPlugin;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class MySQLManagerImpl implements MySQLManager {
    ConcurrentHashMap<String, MySQLPool> map = new ConcurrentHashMap<>();

    public MySQLManagerImpl(List<SimpleConfig> configList) {
        Objects.requireNonNull(configList);
        for (SimpleConfig simpleConfig : configList) {
            String name = simpleConfig.getName();
            MySQLPool pool = getMySQLPool(simpleConfig.getPort(), simpleConfig.getHost(), simpleConfig.getDatabase(), simpleConfig.getUser(), simpleConfig.getPassword(), simpleConfig.getMaxSize());
            map.put(name,pool);
        }
    }

    private MySQLPool getMySQLPool(int port, String host, String database, String user, String password, int maxSize) {
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setPort(port)
                .setAuthenticationPlugin(MySQLAuthenticationPlugin.MYSQL_NATIVE_PASSWORD)
                .setHost(host)
                .setDatabase(database)
                .setUser(user)
                .setPassword(password);
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(maxSize);
        return MySQLPool.pool(connectOptions, poolOptions);
    }


    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        return map.get(targetName).getConnection();
    }

    @Override
    public void close(Handler<Future> handler) {
        map.values().stream().forEach(c->c.close());
        handler.handle(Future.succeededFuture());
    }
}
