

package cn.mycat.vertx.xa.log;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface Repository {
    default long getTimeout() {
        return TimeUnit.SECONDS.toMillis(30);
    }

    void init();

    void put(String id, ImmutableCoordinatorLog coordinatorLog);

    void remove(String id);

    ImmutableCoordinatorLog get(String coordinatorId);

    Collection<ImmutableCoordinatorLog> getAllCoordinatorLogEntries(boolean first);

    void close();

}
