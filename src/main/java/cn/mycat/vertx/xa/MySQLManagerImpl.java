package cn.mycat.vertx.xa;

import io.vertx.core.CompositeFuture;
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
import java.util.stream.Collectors;

public class MySQLManagerImpl implements MySQLManager {
    private final ConcurrentHashMap<String, MySQLPool> nameMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> hostMap = new ConcurrentHashMap<>();

    public MySQLManagerImpl(List<SimpleConfig> configList) {
        Objects.requireNonNull(configList);
        for (SimpleConfig simpleConfig : configList) {
            String name = simpleConfig.getName();
            MySQLPool pool = getMySQLPool(simpleConfig.getPort(), simpleConfig.getHost(), simpleConfig.getDatabase(), simpleConfig.getUser(), simpleConfig.getPassword(), simpleConfig.getMaxSize());
            nameMap.put(name, pool);
            hostMap.put(simpleConfig.getHost() + ":" + simpleConfig.getPort(), name);
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
        return nameMap.get(targetName).getConnection();
    }

    public String getDatasourceName(String host, int port) {
        return hostMap.get(host + ":" + port);
    }

    @Override
    public Future<SqlConnection> getConnection(String host, int port) {
        return nameMap.get(getDatasourceName(host,port)).getConnection();
    }

    @Override
    public void close(Handler<Future> handler) {
        CompositeFuture.all(nameMap.values().stream().map(i -> i.close()).collect(Collectors.toList()))
                .onComplete(event -> handler.handle(event.result()));
    }
}
