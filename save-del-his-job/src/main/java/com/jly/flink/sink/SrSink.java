package com.jly.flink.sink;

import com.google.common.collect.Maps;
import com.jly.flink.config.SinkConfig;
import com.jly.flink.config.TaskConfig;
import com.jly.flink.model.TargetDataRow;
import com.jly.flink.utils.SrStreamLoadClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xiaoyw
 * @date 2025/11/6 12:47
 * @description SR Sink
 */
@Slf4j
public class SrSink extends RichSinkFunction<TargetDataRow> implements CheckpointedFunction {
    private final TaskConfig taskConfig;
    private final Map<String, TaskConfig.SourceInfo> SOURCE_MAP;
    private final SinkConfig sinkConfig;
    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;

    // 状态：缓存待写入的数据（按表名分组）
    private transient ConcurrentMap<String, List<TargetDataRow>> buffer;
    private transient ListState<BufferItem> checkpointedState;
    private transient SrStreamLoadClient srStreamLoadClient;
    // 当前 checkpoint ID（用于生成幂等 label）
    private AtomicLong currentCheckpointId = new AtomicLong(0L);

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
        srStreamLoadClient = new SrStreamLoadClient(sinkConfig);
        buffer = Maps.newConcurrentMap();

        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "sr-data-flush-thread");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleAtFixedRate(() -> {
            try {
                flushAll();
            } catch (Exception e) {
                log.error("Flushing data to SR failed", e);
            }
        }, sinkConfig.getFlushIntervalMs(), sinkConfig.getFlushIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(TargetDataRow value, Context context) throws Exception {
        if (closed) {
            return;
        }

        synchronized (SrSink.class) {
            TaskConfig.SourceInfo sourceInfo = SOURCE_MAP.get(value.getInstanceName());
            value.setDbTbName(String.format("%s_%s", taskConfig.getDbAlias(), value.getTableName()));
            value.getDataRow().setFbNo(sourceInfo.getFbNo());

            buffer.computeIfAbsent(value.getDbTbName(), k -> new ArrayList<>()).add(value);

            // 触发批量写入
            if (buffer.get(value.getDbTbName()).size() >= sinkConfig.getBatchSize()) {
                flushTable(value.getDbTbName());
            }
        }
    }

    private void flushTable(String dbTbName) throws Exception {
        List<TargetDataRow> rows = buffer.remove(dbTbName);
        if (CollectionUtils.isEmpty(rows)) {
            return;
        }

        // 使用 checkpointId 生成幂等 label
        String label = String.format("sink_sr_%s_%d_%d",
                getRuntimeContext().getJobInfo().getJobId().toString().replace("-", ""),
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                currentCheckpointId.getAndIncrement());

        String resp;
        try {
            resp = srStreamLoadClient.streamLoad(dbTbName, label, rows);
        } catch (Exception e) {
            log.error("Flushing to SR table {} failed, rows={}, label={}", dbTbName , rows.size(), label);
            throw e;
        }

        if(!resp.contains("\"Status\": \"Success\"")) {
            throw new RuntimeException(String.format("Flushing to SR table %s failed, rows=%d, label=%s, resp: %s", dbTbName, rows.size(), label, resp));
        }
        log.info("Flushing to SR table {} success, rows={}, label={}", dbTbName, rows.size(), label);
    }

    private void flushAll() throws Exception {
        synchronized (SrSink.class) {
            for (String dbTbName : buffer.keySet()) {
                flushTable(dbTbName);
            }
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (Objects.nonNull(scheduler)) {
            scheduler.shutdownNow();
        }

        flushAll();
        srStreamLoadClient.close();
    }

    // ========================= Checkpoint =========================
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("Snapshotting state..., checkpointId={}", context.getCheckpointId());

        // 1. 设置当前 checkpoint ID（用于 label 生成）
        long newCheckpointId = context.getCheckpointId();
        long currentId = currentCheckpointId.get();
        if (newCheckpointId > currentId) {
            this.currentCheckpointId.set(newCheckpointId);
        }

        // 2. 先 flush 所有数据，确保所有数据已提交（幂等）
        flushAll();

        // 3. 将当前 buffer 状态保存到 Flink State（用于故障恢复）
        checkpointedState.clear();
        for (Map.Entry<String, List<TargetDataRow>> entry : buffer.entrySet()) {
            checkpointedState.add(new BufferItem(entry.getKey(), new ArrayList<>(entry.getValue())));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化状态
        ListStateDescriptor<BufferItem> descriptor = new ListStateDescriptor<>("buffer-state", TypeInformation.of(new TypeHint<>() {}));
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        // 恢复状态（故障恢复时）
        buffer = new ConcurrentHashMap<>();
        if (context.isRestored()) {
            for (BufferItem item : checkpointedState.get()) {
                buffer.put(item.tableName, item.rows);
            }
        }
    }

    /**
     * 状态存储单元（必须实现序列化）
     */
    public static class BufferItem implements Serializable {
        private static final long serialVersionUID = 1L;
        public String tableName;
        public List<TargetDataRow> rows;

        public BufferItem() {
        }

        public BufferItem(String tableName, List<TargetDataRow> rows) {
            this.tableName = tableName;
            this.rows = rows;
        }
    }
}
