package cn.mycat.vertx.xa;

import net.jcip.annotations.NotThreadSafe;

import java.util.Arrays;
import java.util.function.BiFunction;

@net.jcip.annotations.NotThreadSafe
public class LocalXaTest extends XaTestSuite {
    public LocalXaTest() throws Exception {
        super(new MySQLManagerImpl(Arrays.asList(demoConfig("ds1", 3307)
                , demoConfig("ds2", 3306))), new BiFunction<MySQLManager, XaLog, XaSqlConnection>() {
            @Override
            public XaSqlConnection apply(MySQLManager mySQLManager, XaLog xaLog) {
                return new LocalXaSqlConnection(mySQLManager, xaLog);
            }
        });
    }
};