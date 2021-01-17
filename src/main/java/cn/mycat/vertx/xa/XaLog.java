/**
 * Copyright [2021] [chen junwen]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.io.Closeable;

public interface XaLog extends AutoCloseable, Closeable {

    /**
     * xid
     * @return
     */
    String nextXid();

    /**
     * Transcation timeout;
     * @return
     */
    long getTimeout();

    /**
     * getTimeout+now;
     * @return
     */
    long getExpires();

    /**
     * time of rollback or commit retry delay
     * @return
     */
    long retryDelay();


    void beginXa(String xid);

    void log(String xid, ImmutableParticipantLog[] participantLogs);

    void log(String xid, String target, State state);

    void logRollback(String xid, boolean succeed);

    /**
     * all participants ared prepared.only for check.
     * @param xid
     * @param succeed
     */
    void logPrepare(String xid, boolean succeed);

    /**
     * all participants ared commited.only for check.
     * @param xid
     * @param succeed
     */
    void logCommit(String xid, boolean succeed);

    /**
     * Need distributed order, persistence.for recover.
     * @param xid
     */
    void logCommitBeforeXaCommit(String xid);

    /**
     * Need distributed order, persistence.for recover.
     * @param xid
     */
    void logCancelCommitBeforeXaCommit(String xid);

    public void readXARecoveryLog(Handler<AsyncResult<XaLog>> handler);
}
