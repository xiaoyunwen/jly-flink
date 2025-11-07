package com.jly.flink.config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author xiaoyw
 * @date 2025/11/6 10:29
 * @description
 */
@Data
public class TaskConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private List<SourceInfo> sources;
    private List<String> tables;
    private String dbAlias;

    @Data
    public static class SourceInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private String instanceName;
        private String serverId;
        private String host;
        private int port;
        private String username;
        private String password;
        private String fbNo;
        private String dbName;
    }
}
