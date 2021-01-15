package cn.mycat.vertx.xa.log;

import java.util.Collection;

public class XaRepository implements Repository {
    final Repository memoryRepository = new MemoryRepositoryImpl();
    final Repository persistenceRepository;

    public XaRepository(Repository persistenceRepository) {
        this.persistenceRepository = persistenceRepository;
    }

    @Override
    public void init() {
        memoryRepository.init();
        persistenceRepository.init();
    }

    @Override
    public void put(String id, ImmutableCoordinatorLog coordinatorLog) {
        memoryRepository.put(id, coordinatorLog);
    }

    @Override
    public void remove(String id) {
        memoryRepository.remove(id);
    }

    @Override
    public ImmutableCoordinatorLog get(String coordinatorId) {
        return memoryRepository.get(coordinatorId);
    }

    @Override
    public Collection<ImmutableCoordinatorLog> getAllCoordinatorLogEntries(boolean first) {
        return persistenceRepository.getAllCoordinatorLogEntries(first);
    }

    @Override
    public void close() {
        memoryRepository.close();
        persistenceRepository.close();
    }
}
