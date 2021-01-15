package cn.mycat.vertx.xa.log;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryRepositoryImpl implements Repository {

    private final Map<String, ImmutableCoordinatorLog> storage = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private boolean closed = true;

    @Override
    public void init() {
        closed = false;
    }

    @Override
    public void put(String id, ImmutableCoordinatorLog coordinatorLog) {
        lock.lock();
        try {
            storage.put(id, coordinatorLog);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ImmutableCoordinatorLog get(String coordinatorId) {
        return storage.get(coordinatorId);
    }

    @Override
    public void close() {
        lock.lock();
        try {
            storage.clear();
        } finally {
            closed = true;
            lock.unlock();
        }

    }

    @Override
    public Collection<ImmutableCoordinatorLog> getAllCoordinatorLogEntries(boolean first) {
        return storage.values();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void remove(String id) {
        lock.lock();
        try {
            ImmutableCoordinatorLog coordinatorLogEntry = storage.get(id);
            if (coordinatorLogEntry != null) {
                switch (coordinatorLogEntry.computeState()) {
                    case XA_COMMITED:
                    case XA_ROLLBACKED:
                        storage.remove(id);
                        break;
                    default:
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
