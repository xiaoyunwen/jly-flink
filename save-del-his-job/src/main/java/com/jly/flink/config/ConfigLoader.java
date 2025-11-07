package com.jly.flink.config;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.Objects;

/**
 * @author xiaoyw
 * @date 2025/11/6 10:32
 * @description
 */
public class ConfigLoader {
    public static <T> T load(String resourcePath, Class<T> clazz) {
        try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (Objects.isNull(is)) {
                throw new RuntimeException("Config file not found: " + resourcePath);
            }

            Yaml yaml = new Yaml(new Constructor(clazz, new LoaderOptions()));
            return yaml.load(is);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config from " + resourcePath, e);
        }
    }
}
