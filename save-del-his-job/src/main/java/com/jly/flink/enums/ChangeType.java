package com.jly.flink.enums;

import lombok.Getter;

/**
 * @author xiaoyw
 * @date 2025/11/6 13:02
 * @description 记录变更类型
 */
@Getter
public enum ChangeType {
    INSERT("i"),

    DELETE("d"),

    UPDATE("u");

    private final String type;

    ChangeType(String type) {
        this.type = type;
    }
}
