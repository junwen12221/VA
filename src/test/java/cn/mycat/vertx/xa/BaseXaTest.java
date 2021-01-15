package cn.mycat.vertx.xa;

import cn.mycat.vertx.xa.log.XaRepository;

import java.util.Arrays;
import java.util.function.BiFunction;

@net.jcip.annotations.NotThreadSafe
public class BaseXaTest extends XaTestSuite {
    public BaseXaTest() throws Exception {
        super(new MySQLManagerImpl(Arrays.asList(demoConfig("ds1", 3306)
                , demoConfig("ds2", 3307))), new BiFunction<MySQLManager, XaLog, XaSqlConnection>() {
            @Override
            public XaSqlConnection apply(MySQLManager mySQLManager, XaLog xaLog) {
                return new BaseXaSqlConnection(mySQLManager, xaLog);
            }
        });
    }
}
