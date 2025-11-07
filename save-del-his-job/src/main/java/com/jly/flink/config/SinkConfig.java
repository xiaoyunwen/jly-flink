package com.jly.flink.config;

import lombok.Data;

import java.io.Serializable;

/**
 * @author xiaoyw
 * @date 2025/11/7 14:59
 * @description
 */
@Data
public class SinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String host;
    private int port;
    private String username;
    private String password;
    private String dbName;
    private int batchSize = 100;
    private long flushIntervalMs = 5_000;
}
