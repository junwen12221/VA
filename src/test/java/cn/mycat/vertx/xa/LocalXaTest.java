package cn.mycat.vertx.xa;

import java.util.Arrays;
import java.util.function.BiFunction;

public class LocalXaTest extends XaTestSuite {
    public LocalXaTest() throws Exception {
        super(new MySQLManagerImpl(Arrays.asList(demoConfig("ds1", 3307)
                , demoConfig("ds2", 3306))), new XaLogImpl(), new BiFunction<MySQLManager, XaLog, XaSqlConnection>() {
            @Override
            public XaSqlConnection apply(MySQLManager mySQLManager, XaLog xaLog) {
                return new LocalXaSqlConnection(mySQLManager, xaLog);
            }
        });
    }
};