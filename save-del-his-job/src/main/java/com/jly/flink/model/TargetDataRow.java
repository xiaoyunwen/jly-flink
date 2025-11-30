package com.jly.flink.model;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @author xiaoyw
 * @date 2025/11/10 12:03
 * @description
 */
@Data
public class TargetDataRow implements Serializable {
    private static final long serialVersionUID = 1L;

    private String instanceName;
    private String tableName;
    private String dbTbName;
    private DataRow dataRow;

    @Data
    public static class DataRow implements Serializable {
        private static final long serialVersionUID = 1L;

        @JSONField(name = "id")
        private String id;

        @JSONField(name = "record_del_time", format = "yyyy-MM-dd HH:mm:ss")
        private Timestamp recordDelTime;

        @JSONField(name = "fb_no")
        private String fbNo;

        @JSONField(name = "data_json")
        private String dataJson;
    }
}
