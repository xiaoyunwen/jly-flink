package com.jly.flink.sink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jly.flink.config.SinkConfig;
import com.jly.flink.config.TaskConfig;
import com.jly.flink.model.TargetDataRow;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
 * @description ADB Sink
 */
@Slf4j
public class AdbSink extends RichSinkFunction<TargetDataRow> {
    private transient volatile boolean closed = false;
    private final SinkConfig sinkConfig;
    private final TaskConfig taskConfig;
    private final Map<String, TaskConfig.SourceInfo> SOURCE_MAP;
    private transient List<TargetDataRow> buffer;
    private transient ScheduledExecutorService scheduler;
    private transient HikariDataSource dataSource;

    public AdbSink(TaskConfig taskConfig, SinkConfig sinkConfig) {
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
            Thread t = new Thread(r, "adb-data-flush-thread");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(this::flush, sinkConfig.getFlushIntervalMs(), sinkConfig.getFlushIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private HikariConfig getDataSourceConfig() {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true",
                sinkConfig.getHost(), sinkConfig.getPort(), sinkConfig.getDbName());

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(sinkConfig.getUsername());
        config.setPassword(sinkConfig.getPassword());
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(30_000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        return config;
    }

    @Override
    public void invoke(TargetDataRow value, Context context) {
        if (closed) {
            return;
        }

        synchronized (AdbSink.class) {
            TaskConfig.SourceInfo sourceInfo = SOURCE_MAP.get(value.getInstanceName());
            value.setDbTbName(String.format("%s_%s", taskConfig.getDbAlias(), value.getTableName()));
            value.getDataRow().setFbNo(sourceInfo.getFbNo());
            buffer.add(value);
            if (buffer.size() >= sinkConfig.getBatchSize()) {
                flush();
            }
        }
    }

    private void flush() {
        synchronized (AdbSink.class) {
            if (buffer.isEmpty()) {
                return;
            }

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                var grouped = buffer.stream().collect(Collectors.groupingBy(TargetDataRow::getDbTbName));

                for (var entry : grouped.entrySet()) {
                    String table = entry.getKey();
                    List<TargetDataRow> records = entry.getValue();
                    String sql = String.format("INSERT INTO `%s` (id, fb_no, record_del_time, data_json) VALUES (?, ?, ?, ?)", table);

                    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                        for (TargetDataRow row : records) {
                            TargetDataRow.DataRow dataRow = row.getDataRow();
                            stmt.setString(1, dataRow.getId());
                            stmt.setString(2, dataRow.getFbNo());
                            stmt.setTimestamp(3, dataRow.getRecordDelTime());
                            stmt.setString(4, dataRow.getDataJson());
                            stmt.addBatch();
                        }
                        stmt.executeBatch();
                    }
                    log.info("Flush to ADB success: table={}, size={}", table, records.size());
                }
                conn.commit();
                buffer.clear();
            } catch (Exception e) {
                log.error("Flush to ADB failed: {}", e.getMessage(), e);
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
