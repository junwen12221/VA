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

import cn.mycat.vertx.xa.log.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.log.ImmutableParticipantLog;

import java.io.Closeable;

public interface XaLog extends AutoCloseable , Closeable {
    void log(String xid, ImmutableParticipantLog[] participantLogs);

    void log(String xid, String target, State state);

    String nextXid();

    long getTimeout();

    void logRollback(String xid, boolean succeed);

    void logPrepare(String xid, boolean succeed);

    void logCommit(String xid, boolean succeed);

    void logCommitBeforeXaCommit(String xid);
    void logCancelLocalCommitBeforeXaCommit(String xid);
    void beginXa(String xid);

    long getExpires();

    long retryDelay();
}
