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

package cn.mycat.vertx.xa.log;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface Repository {
    default long getTimeout() {
        return TimeUnit.SECONDS.toMillis(30);
    }

    default long retryDelay() {
        return TimeUnit.SECONDS.toMillis(3);
    }

    void init();

    void put(String id, ImmutableCoordinatorLog coordinatorLog);

    void remove(String id);

    ImmutableCoordinatorLog get(String coordinatorId);

    Collection<ImmutableCoordinatorLog> getCoordinatorLogs();

    void close();

   default void writeCommitedLog(ImmutableCoordinatorLog immutableCoordinatorLog){
        put(immutableCoordinatorLog.getXid(),immutableCoordinatorLog);
    }
    default void cancelCommitedLog(String xid){
        ImmutableCoordinatorLog immutableCoordinatorLog = get(xid);
        immutableCoordinatorLog.withCommited(false);
        put(xid,immutableCoordinatorLog);
    }
}
