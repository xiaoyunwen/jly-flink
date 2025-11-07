package com.jly.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @author xiaoyw
 * @date 2025/11/6 10:57
 * @description
 */
@Data
@AllArgsConstructor
public class DelHistory implements Serializable {
    private static final long serialVersionUID = 1L;
    private String dbTbName;
    private String id;
    private String fbNo;
    private String dataJson;
    private Timestamp delTime;
}
