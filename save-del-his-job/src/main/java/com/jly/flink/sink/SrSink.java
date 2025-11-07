package com.jly.flink.sink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jly.flink.config.SinkConfig;
import com.jly.flink.config.TaskConfig;
import com.jly.flink.model.DelHistory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author xiaoyw
 * @date 2025/11/6 12:47
 * @description SR Sink
 */
@Slf4j
public class SrSink extends RichSinkFunction<Tuple5<String, String, String, String, Timestamp>> {
    private transient volatile boolean closed = false;
    private final TaskConfig taskConfig;
    private final Map<String, TaskConfig.SourceInfo> SOURCE_MAP;
    private final SinkConfig sinkConfig;
    private transient List<DelHistory> buffer;
    private transient ScheduledExecutorService scheduler;
    private transient HikariDataSource dataSource;

    public SrSink(TaskConfig taskConfig, SinkConfig sinkConfig) {
        this.taskConfig = taskConfig;
        this.sinkConfig = sinkConfig;

        this.SOURCE_MAP = Maps.newHashMap();
        for (TaskConfig.SourceInfo source : taskConfig.getSources()) {
            SOURCE_MAP.put(source.getInstanceName(), source);
        }
    }

    @Override
    public void open(Configuration parameters) {
        this.dataSource = new HikariDataSource(getDataSourceConfig());
        this.buffer = Lists.newArrayListWithCapacity(sinkConfig.getBatchSize());

        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "data-flush-thread");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(this::flush, sinkConfig.getFlushIntervalMs(), sinkConfig.getFlushIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private HikariConfig getDataSourceConfig() {
        String httpUrl = String.format("http://%s:%d", sinkConfig.getHost(), sinkConfig.getPort());

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(httpUrl);
        config.setUsername(sinkConfig.getUsername());
        config.setPassword(sinkConfig.getPassword());
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30_000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        return config;
    }

    @Override
    public void invoke(Tuple5<String, String, String, String, Timestamp> value, Context context) {
        if (closed) {
            return;
        }

        synchronized (taskConfig) {
            // Tuple5<>(instanceName, tableName, id, dataJson, delTime)
            TaskConfig.SourceInfo sourceInfo = SOURCE_MAP.get(value.f0);
            String dbTbName = String.format("%s_%s", taskConfig.getDbAlias(), value.f1);
            buffer.add(new DelHistory(dbTbName, value.f2, sourceInfo.getFbNo(), value.f3, value.f4));
            if (buffer.size() >= sinkConfig.getBatchSize()) {
                flush();
            }
        }
    }

    private void flush() {
        synchronized (taskConfig) {
            if (buffer.isEmpty()) {
                return;
            }

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                var grouped = buffer.stream().collect(Collectors.groupingBy(DelHistory::getDbTbName));

                for (var entry : grouped.entrySet()) {
                    String table = entry.getKey();
                    List<DelHistory> records = entry.getValue();
                    String sql = String.format("INSERT INTO `%s` (id, fb_no, record_del_time, data_json) VALUES (?, ?, ?, ?)", table);

                    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                        for (DelHistory r : records) {
                            stmt.setString(1, r.getId());
                            stmt.setString(2, r.getFbNo());
                            stmt.setTimestamp(3, r.getDelTime());
                            stmt.setString(4, r.getDataJson());
                            stmt.addBatch();
                        }
                        stmt.executeBatch();
                    }
                }
                conn.commit();
                buffer.clear();
            } catch (Exception e) {
                log.error("Flush failed: {}", e.getMessage(), e);
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        if (Objects.nonNull(scheduler)) {
            scheduler.shutdownNow();
        }

        flush();
        if (Objects.nonNull(dataSource)) {
            dataSource.close();
        }
    }
}
