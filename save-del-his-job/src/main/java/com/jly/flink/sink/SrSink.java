package com.jly.flink.sink;

import com.google.common.collect.Lists;
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
import java.util.*;
import java.util.concurrent.*;

/**
 * @author xiaoyw
 * @date 2025/11/6 12:47
 * @description SR Sink
 */
@Slf4j
public class SrSink extends RichSinkFunction<TargetDataRow> implements CheckpointedFunction {
    private static final String LABEL_EXISTS = "Label Already Exists";
    private static final String SUCCESS_STATUS = "\"Status\": \"Success\"";

    private final TaskConfig taskConfig;
    private final Map<String, TaskConfig.SourceInfo> SOURCE_MAP;
    private final SinkConfig sinkConfig;
    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;

    /**
     * 状态：缓存待写入的数据（按表名分组）
     */
    private transient ConcurrentMap<String, List<TargetDataRow>> buffer;
    private transient ListState<BufferItem> checkpointState;
    private transient SrStreamLoadClient srStreamLoadClient;
    /**
     * 实例标识
     */
    private final String UID = UUID.randomUUID().toString().replace("-", "").substring(0, 8);

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
                log.error("Flush to SR failed", e);
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
        List<TargetDataRow> dataRows = buffer.remove(dbTbName);
        if (CollectionUtils.isEmpty(dataRows)) {
            return;
        }

        List<List<TargetDataRow>> partitions = Lists.partition(dataRows, sinkConfig.getBatchSize());
        int idx = 0;
        for (List<TargetDataRow> pt : partitions) {
            String label = String.format("sink_sr_%s_%s_%d_%d_%d",
                    getRuntimeContext().getJobInfo().getJobId().toString().replace("-", ""),
                    UID,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                    System.nanoTime(),
                    idx++);

            String resp;
            try {
                resp = srStreamLoadClient.streamLoad(dbTbName, label, pt);
            } catch (Exception e) {
                log.error("Flush to SR failed: table={}, size={}, label={}, message={}", dbTbName , pt.size(), label, e.getMessage());
                throw e;
            }

            if(resp.contains(LABEL_EXISTS)) {
                log.warn("Flush to SR failed: Label already exists, table={}, size={}, label={}", dbTbName, pt.size(), label);
                return;
            }

            if(!resp.contains(SUCCESS_STATUS)) {
                throw new RuntimeException(String.format("Flush to SR table %s failed, size=%d, label=%s, resp: %s", dbTbName, pt.size(), label, resp));
            }
            log.info("Flush to SR success: table={}, size={}, label={}", dbTbName, pt.size(), label);
        }
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

        // flush所有数据，确保所有数据已提交（幂等）
        flushAll();

        // 将当前 buffer 状态保存到 Flink State（用于故障恢复）
        checkpointState.clear();
        for (Map.Entry<String, List<TargetDataRow>> entry : buffer.entrySet()) {
            checkpointState.add(new BufferItem(entry.getKey(), new ArrayList<>(entry.getValue())));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化状态
        ListStateDescriptor<BufferItem> descriptor = new ListStateDescriptor<>("buffer-state", TypeInformation.of(new TypeHint<>() {}));
        checkpointState = context.getOperatorStateStore().getListState(descriptor);

        // 恢复状态（故障恢复时）
        buffer = Maps.newConcurrentMap();
        if (context.isRestored()) {
            for (BufferItem item : checkpointState.get()) {
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
