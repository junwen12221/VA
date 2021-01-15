package cn.mycat.vertx.xa;

import io.vertx.mysqlclient.SslMode;

import java.util.Map;

public class SimpleConfig {
    private String name;
    private String host;
    private int port;
    private String user;
    private String password;
    private String database;
    private int maxSize;
    public static final boolean DEFAULT_USE_AFFECTED_ROWS = true;
    public SimpleConfig( String host, int port, String user, String password, String database, int maxSize){
        this(host+":"+port,host,port,user,password,database,maxSize);
    }
    public SimpleConfig(String name, String host, int port, String user, String password, String database, int maxSize) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = database;
        this.maxSize = maxSize;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public String getName() {
        return name;
    }

    public String getDatabase() {
        return database;
    }
}
