package com.jly.flink.config;

import org.junit.Test;

/**
 * @author xiaoyw
 * @date 2025/11/6 10:44
 * @description
 */
public class ConfigLoaderTest {
    @Test
    public void testLoad() {
        TaskConfig configInfo = ConfigLoader.load("application-cbs.yaml", TaskConfig.class);
        System.out.println(configInfo);

        SinkConfig adbSinkConf = ConfigLoader.load("sink-adb.yaml", SinkConfig.class);
        System.out.println(adbSinkConf);

        SinkConfig srSinkConf = ConfigLoader.load("sink-sr.yaml", SinkConfig.class);
        System.out.println(srSinkConf);
    }
}
