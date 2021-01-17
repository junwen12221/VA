/**
 * Copyright [2021] [chen junwen]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.mycat.vertx.xa;

import cn.mycat.vertx.xa.impl.BaseXaSqlConnection;
import cn.mycat.vertx.xa.impl.MySQLManagerImpl;

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
