package cn.mycat.vertx.xa.log;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FileRepositoryImpl implements Repository {


    private static final String FILE_SEPARATOR = String.valueOf(File.separatorChar);
    private final String baseDir;
    private final String suffix;
    private final Vertx vertx;
    private Long timeHandler;

    public FileRepositoryImpl(final String baseDir, final String suffix, Vertx vertx) {
        this.baseDir = baseDir;
        this.suffix = suffix;
        this.vertx = vertx;
    }

    @Override
    public void init() {
        this.timeHandler = this.vertx.setPeriodic(TimeUnit.SECONDS.toMillis(5), 
                event -> vertx.executeBlocking(event1 -> {
            try {
                long now = System.currentTimeMillis();
                Files.list(Paths.get(baseDir))
                        .filter(path -> !Files.isDirectory(path) && path.toFile().getPath().endsWith(suffix))
                        .filter(path -> {
                            long l = path.toFile().lastModified();
                            long ret = TimeUnit.MILLISECONDS.toSeconds(now - l);
                            return ret > 1;
                        }).forEach(new Consumer<Path>() {
                    @Override
                    public void accept(Path path) {
                        try {
                            ImmutableCoordinatorLog coordinatorLogEntry = Json.decodeValue(new String(Files.readAllBytes(path)), ImmutableCoordinatorLog.class);
                            switch (coordinatorLogEntry.computeState()) {
                                case XA_COMMITED:
                                case XA_ROLLBACKED:
                                    Files.delete(path);
                                    break;
                            }
                        } catch (Throwable throwable) {

                        }
                    }
                });
            } catch (Throwable e) {

            } finally {
                event1.complete();
            }
        }));
    }

    @Override
    public void put(String id, ImmutableCoordinatorLog coordinatorLog) {
        Path resolve = getPath(id);
        try {
            if (!Files.exists(resolve)) {
                Files.createFile(resolve);
            }
            Files.write(resolve,Json.encode(coordinatorLog).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path getPath(String id) {
        return Paths.get(baseDir).resolve(id + FILE_SEPARATOR + suffix);
    }

    @Override
    public void remove(String id) {
        try {
            Files.deleteIfExists(getPath(id));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ImmutableCoordinatorLog get(String coordinatorId) {
        Path path = getPath(coordinatorId);
        try {
            return   Json.decodeValue(new String(Files.readAllBytes(path)), ImmutableCoordinatorLog.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<ImmutableCoordinatorLog> getAllCoordinatorLogEntries(boolean first) {
        try {
            return Files.list(Paths.get(baseDir))
                    .filter(path -> !Files.isDirectory(path) && path.toFile().getPath().endsWith(suffix))
                    .map(path -> {
                        try {
                            return new String(Files.readAllBytes(path));
                        } catch (IOException e) {
                            throw new RuntimeException(e);

                        }
                    }).map(i -> Json.decodeValue(i, ImmutableCoordinatorLog.class)).collect(Collectors.toList());
        } catch (Throwable throwable) {
            return Collections.emptyList();
        }

    }

    @Override
    public void close() {
        if (timeHandler != null) {
            this.vertx.cancelTimer(this.timeHandler);
            timeHandler = null;
        }

    }
}
