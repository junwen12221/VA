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
    public Collection<ImmutableCoordinatorLog> getCoordinatorLogs() {
        return map.values().stream().map(i->Json.decodeValue(i, ImmutableCoordinatorLog.class)).collect(Collectors.toList());
    }

    @Override
    public void close() {

    }
}
