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
