package cn.mycat.vertx.xa.log;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.Json;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class KvRepositoryImpl implements Repository {
    private final String key;
    private final Map<String, String> map;
    private static final Logger LOGGER = LoggerFactory.getLogger(KvRepositoryImpl.class);

    public KvRepositoryImpl(String key, Map<String, String> map) {
        this.key = key;
        this.map = map;

    }

    @Override
    public void init() {

    }

    @Override
    public void put(String id, ImmutableCoordinatorLog coordinatorLog) {
        this.map.put(id,Json.encode(coordinatorLog));
    }

    @Override
    public void remove(String id) {
        this.map.remove(id);
    }

    @Override
    public ImmutableCoordinatorLog get(String coordinatorId) {
        String s = this.map.get(coordinatorId);
        if (s!=null){
            return Json.decodeValue(s,ImmutableCoordinatorLog.class);
        }
        return null;
    }

    @Override
    public Collection<ImmutableCoordinatorLog> getAllCoordinatorLogEntries(boolean first) {
        return map.values().stream().map(i->Json.decodeValue(i, ImmutableCoordinatorLog.class)).collect(Collectors.toList());
    }

    @Override
    public void close() {

    }
}
