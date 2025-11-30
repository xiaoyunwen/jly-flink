package com.jly.flink.config;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Set;

/**
 * @author xiaoyw
 * @date 2025/11/10 12:07
 * @description
 */
public class TaskConfigChecker {
    public static void check(TaskConfig config) {
        List<String> tables = config.getTables();
        if(CollectionUtils.isEmpty(tables)) {
            throw new IllegalArgumentException("tables is empty");
        }

        List<TaskConfig.SourceInfo> sources = config.getSources();
        if(CollectionUtils.isEmpty(sources)) {
            throw new IllegalArgumentException("sources is empty");
        }

        Set<String> instanceNames = Sets.newHashSet();
        for (TaskConfig.SourceInfo source : sources) {
            if(instanceNames.contains(source.getInstanceName())) {
                throw new IllegalArgumentException("instanceName is duplicated: " + source.getInstanceName());
            } else {
                instanceNames.add(source.getInstanceName());
            }
        }
    }
}
