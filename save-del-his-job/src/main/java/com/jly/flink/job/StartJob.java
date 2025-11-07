package com.jly.flink.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jly.flink.config.ConfigLoader;
import com.jly.flink.config.SinkConfig;
import com.jly.flink.config.TaskConfig;
import com.jly.flink.enums.ChangeType;
import com.jly.flink.sink.AdbSink;
import com.jly.flink.sink.SrSink;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @author xiaoyw
 * @date 2025/11/5 19:00
 * @description
 */
@Slf4j
public class StartJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);
        env.setParallelism(1);

        // 从变量中获取需要加载的配置文件
        ParameterTool params = ParameterTool.fromArgs(args);
        String dbListened = StringUtils.isNotBlank(params.get("db_listened")) ? "-" + params.get("db_listened") : "";
        String configFile = String.format("application%s.yaml", dbListened);
        log.info("configFile = {}", configFile);
        TaskConfig config = ConfigLoader.load(configFile, TaskConfig.class);
        List<TaskConfig.SourceInfo> sources = config.getSources();
        List<DataStream<Tuple5<String, String, String, String, Timestamp>>> streams = new ArrayList<>();

        for (TaskConfig.SourceInfo source : sources) {
            log.info("sourceInfo.instanceName = {}", source.getInstanceName());

            String[] fullTableNames = config.getTables().stream()
                    .map(tb -> source.getDbName() + "." + tb)
                    .toArray(String[]::new);

            // 配置Debezium属性，处理Decimal类型（防止decimal类型被转为Base64编码）
            Properties debeziumProps = new Properties();
            debeziumProps.setProperty("decimal.handling.mode", "string");

            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                    .hostname(source.getHost())
                    .port(source.getPort())
                    .databaseList(source.getDbName())
                    .tableList(fullTableNames)
                    .username(source.getUsername())
                    .password(source.getPassword())
                    .serverId(source.getServerId())
                    .startupOptions(StartupOptions.latest())
                    .debeziumProperties(debeziumProps)
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .build();

            DataStream<String> rawStream = env.fromSource(
                    mySqlSource,
                    WatermarkStrategy.noWatermarks(),
                    "SRC-" + source.getInstanceName()
            );

            DataStream<Tuple5<String, String, String, String, Timestamp>> parsed =
                    rawStream.map(new ParseDeleteWithSource(source.getInstanceName()))
                            .filter(Objects::nonNull);
            streams.add(parsed);
        }

        // 合并所有流
        DataStream<Tuple5<String, String, String, String, Timestamp>> unionStream = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
            unionStream = unionStream.union(streams.get(i));
        }

        // 添加ADB Sink
        SinkConfig adbSinkConf = ConfigLoader.load("sink-adb.yaml", SinkConfig.class);
        unionStream.addSink(new AdbSink(config, adbSinkConf))
                .name("adb-sink")
                .setParallelism(1);

        // 添加SR Sink
        boolean sinkToSr = params.getBoolean("sink_to_sr", false);
        if(sinkToSr) {
            // TODO 修改SR Sink类
            log.info("Add SR sink...");
            //SinkConfig srSinkConf = ConfigLoader.load("sink-sr.yaml", SinkConfig.class);
            //unionStream.addSink(new SrSink(config, srSinkConf)).name("sr-sink").setParallelism(1);
        }

        log.info("start job...");
        env.execute("save-del-his-job");
    }

    /**
     * 解析器
     */
    public static class ParseDeleteWithSource implements MapFunction<String, Tuple5<String, String, String, String, Timestamp>> {
        private final String instanceName;

        public ParseDeleteWithSource(String instanceName) {
            this.instanceName = instanceName;
        }

        @Override
        public Tuple5<String, String, String, String, Timestamp> map(String value) {
            JSONObject root = JSONObject.parseObject(value);
            if (!ChangeType.DELETE.getType().equals(root.getString("op"))) {
                return null;
            }

            JSONObject sourceNode = root.getJSONObject("source");
            String tableName = sourceNode.getString("table");
            if (StringUtils.isBlank(tableName)) {
                log.error("table name is empty, table: {}", tableName);
                return null;
            }

            JSONObject beforeNode = root.getJSONObject("before");
            if (Objects.isNull(beforeNode)) {
                log.error("record before is null, table: {}", tableName);
                return null;
            }

            Object id = beforeNode.get("id");
            if (Objects.isNull(id)) {
                log.error("record id is null, table: {}", tableName);
                return null;
            }

            String dataJson = beforeNode.toString();
            long tsMs = root.getLongValue("ts_ms", 0L);
            if (tsMs <= 0) {
                log.error("ts_ms is null, table = {}", tableName);
                return null;
            }
            return new Tuple5<>(instanceName, tableName, id.toString(), dataJson, new Timestamp(tsMs));
        }
    }
}
