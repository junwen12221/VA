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
    public Collection<ImmutableCoordinatorLog> getCoordinatorLogs() {
        return persistenceRepository.getCoordinatorLogs();
    }

    @Override
    public void close() {
        memoryRepository.close();
        persistenceRepository.close();
    }
}
