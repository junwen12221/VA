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

import io.vertx.sqlclient.Row;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class ProcessMonitor {

    public void onRow(Row row) {

    }

    public void onFinish() {

    }

    public void onThrowable(Throwable throwable) {

    }
    private Collector<Row, Void, Void> getCollector(ProcessMonitor monitor) {
        return new Collector<Row, Void, Void>() {
            @Override
            public Supplier<Void> supplier() {
                return () -> null;
            }

            @Override
            public BiConsumer<Void, Row> accumulator() {
                return (aVoid, row) -> monitor.onRow(row);
            }

            @Override
            public BinaryOperator<Void> combiner() {
                return (aVoid, aVoid2) -> null;
            }

            @Override
            public Function<Void, Void> finisher() {
                return aVoid -> {
                    monitor.onFinish();
                    return null;
                };
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Collections.emptySet();
            }
        };
    }
}
