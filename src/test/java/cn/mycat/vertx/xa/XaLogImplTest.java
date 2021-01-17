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

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.util.JdbcUtils;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import static cn.mycat.vertx.xa.XaTestSuite.*;

@net.jcip.annotations.NotThreadSafe
@ExtendWith(VertxExtension.class)
public class XaLogImplTest {

    @Test
    public void demo(VertxTestContext testContext) throws Exception {
        XaLogImpl demoRepository = getDemoRepository();
        demoRepository.performXARecoveryLog(event -> {
            Assertions.assertEquals("x.0", demoRepository.nextXid());
            Assertions.assertEquals("x.1", demoRepository.nextXid());
            testContext.completeNow();
        });
    }

    @Test
    public void demo2(VertxTestContext testContext) throws Exception {
        XaLogImpl demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.performXARecoveryLog(event -> {
                try {
                    extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
                    extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_COMMIT_ONE_PHASE);
                    testContext.completeNow();
                } catch (SQLException throwables) {
                    testContext.failNow(throwables);
                } finally {
                    JdbcUtils.close(mySQLConnection);
                }
            });
        }
    }

    @Test
    public void demo3(VertxTestContext testContext) throws SQLException, IOException {
        XaLogImpl demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.log(xid, "ds1", State.XA_ENDED);
            demoRepository.performXARecoveryLog(event -> {
                try {
                    extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_COMMIT_ONE_PHASE);
                    testContext.completeNow();
                } catch (SQLException throwables) {
                    testContext.failNow(throwables);
                } finally {
                    JdbcUtils.close(mySQLConnection);
                }
            });
        }

    }

    private XaLogImpl getDemoRepository() {
        return XaLogImpl.createDemoRepository(new MySQLManagerImpl(Arrays.asList(demoConfig("ds1", 3306)
                , demoConfig("ds2", 3307))));
    }

    @Test
    public void demo4(VertxTestContext testContext) throws Exception {
        Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
        try {
            extracteXaCmd("x.0", mySQLConnection, XaSqlConnection.XA_COMMIT);
        } catch (Throwable i) {
        }
        XaLogImpl demoRepository = getDemoRepository();
        String xid = demoRepository.nextXid();
        demoRepository.beginXa(xid);

        extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
        extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
        extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_PREPARE);
        demoRepository.log(xid, "ds1", State.XA_STARTED);
        demoRepository.log(xid, "ds1", State.XA_ENDED);
        demoRepository.log(xid, "ds1", State.XA_PREPARED);
        forceClose((DruidPooledConnection) mySQLConnection);
        demoRepository.performXARecoveryLog(event -> {
            try {
                Connection
                        connection = XaTestSuite.getMySQLConnection(DB1);
                Assertions.assertTrue(
                        JdbcUtils
                                .executeQuery(connection, XaSqlConnection.XA_RECOVER, Collections.emptyList()).isEmpty());
                JdbcUtils.close(connection);
                testContext.completeNow();
            } catch (Exception throwables) {
                testContext.failNow(throwables);
            } finally {
                JdbcUtils.close(mySQLConnection);
            }
        });
    }

    private void forceClose(DruidPooledConnection mySQLConnection) throws SQLException {
        DruidPooledConnection mySQLConnection1 = mySQLConnection;
        mySQLConnection1.getConnection().close();
        mySQLConnection1.abandond();
        mySQLConnection1.close();
    }

    @Test
    public void demo5(VertxTestContext testContext) throws Exception {
        XaLogImpl demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_PREPARE);
            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_COMMIT);
            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.log(xid, "ds1", State.XA_ENDED);
            demoRepository.log(xid, "ds1", State.XA_PREPARED);
            demoRepository.log(xid, "ds1", State.XA_COMMITED);
            demoRepository.performXARecoveryLog(event -> {
                try {
                    Assertions.assertTrue(
                            JdbcUtils
                                    .executeQuery(mySQLConnection, XaSqlConnection.XA_RECOVER, Collections.emptyList()).isEmpty());

                    testContext.completeNow();
                } catch (SQLException throwables) {
                    testContext.failNow(throwables);
                } finally {
                    JdbcUtils.close(mySQLConnection);
                }
            });
        }
    }
    @Test
    public void demo6(VertxTestContext testContext) throws Exception {
        XaLogImpl demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            Connection mySQLConnection2 = XaTestSuite.getMySQLConnection(DB2);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_START);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_END);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_PREPARE);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_PREPARE);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_COMMIT);
            forceClose((DruidPooledConnection) mySQLConnection2);

            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.log(xid, "ds1", State.XA_ENDED);
            demoRepository.log(xid, "ds1", State.XA_PREPARED);
            demoRepository.log(xid, "ds1", State.XA_COMMITED);

            demoRepository.log(xid, "ds2", State.XA_STARTED);
            demoRepository.log(xid, "ds2", State.XA_ENDED);
            demoRepository.log(xid, "ds2", State.XA_PREPARED);


            demoRepository.performXARecoveryLog(event -> {
                try {
                    Connection mySQLConnection1 = getMySQLConnection(DB2);
                    Assertions.assertTrue(
                            JdbcUtils
                                    .executeQuery(mySQLConnection1, XaSqlConnection.XA_RECOVER, Collections.emptyList()).isEmpty());
                    JdbcUtils.close(mySQLConnection1);
                    testContext.completeNow();
                } catch (SQLException throwables) {
                    testContext.failNow(throwables);
                } finally {
                    JdbcUtils.close(mySQLConnection);
                }
            });
        }
    }

    @Test
    public void demo7(VertxTestContext testContext) throws Exception {
        XaLogImpl demoRepository = getDemoRepository();
        {
            String xid = demoRepository.nextXid();
            demoRepository.beginXa(xid);
            Connection mySQLConnection = XaTestSuite.getMySQLConnection(DB1);
            Connection mySQLConnection2 = XaTestSuite.getMySQLConnection(DB2);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_START);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_START);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_END);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_END);

            extracteXaCmd(xid, mySQLConnection, XaSqlConnection.XA_PREPARE);
            extracteXaCmd(xid, mySQLConnection2, XaSqlConnection.XA_PREPARE);

            forceClose((DruidPooledConnection) mySQLConnection);
            forceClose((DruidPooledConnection) mySQLConnection2);

            demoRepository.log(xid, "ds1", State.XA_STARTED);
            demoRepository.log(xid, "ds1", State.XA_ENDED);
            demoRepository.log(xid, "ds1", State.XA_PREPARED);


            demoRepository.log(xid, "ds2", State.XA_STARTED);
            demoRepository.log(xid, "ds2", State.XA_ENDED);
            demoRepository.log(xid, "ds2", State.XA_PREPARED);


            demoRepository.performXARecoveryLog(event -> {
                try {
                    Connection mySQLConnection1 = getMySQLConnection(DB2);
                    Assertions.assertTrue(
                            JdbcUtils
                                    .executeQuery(mySQLConnection1, XaSqlConnection.XA_RECOVER, Collections.emptyList()).isEmpty());
                    JdbcUtils.close(mySQLConnection1);
                    testContext.completeNow();
                } catch (SQLException throwables) {
                    testContext.failNow(throwables);
                } finally {
                    JdbcUtils.close(mySQLConnection);
                }
            });
        }
    }
    private void extracteXaCmd(String xid, Connection mySQLConnection, String cmd) throws SQLException {
        JdbcUtils.execute(mySQLConnection, String.format(cmd, xid));
    }
}