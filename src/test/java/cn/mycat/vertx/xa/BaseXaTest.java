package cn.mycat.vertx.xa;

import java.util.Arrays;
import java.util.function.BiFunction;

public class BaseXaTest extends XaTestSuite {
    public BaseXaTest() throws Exception {
        super(new MySQLManagerImpl(Arrays.asList(demoConfig("ds1", 3307)
                , demoConfig("ds2", 3306))), new XaLogImpl(), new BiFunction<MySQLManager, XaLog, XaSqlConnection>() {
            @Override
            public XaSqlConnection apply(MySQLManager mySQLManager, XaLog xaLog) {
                return new BaseXaSqlConnection(mySQLManager, xaLog);
            }
        });
    }
}
