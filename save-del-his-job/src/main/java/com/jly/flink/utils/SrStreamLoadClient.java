package com.jly.flink.utils;

import com.alibaba.fastjson2.JSON;
import com.jly.flink.config.SinkConfig;
import com.jly.flink.model.TargetDataRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author xiaoyw
 * @date 2025/11/16 18:17
 * @description
 */
@Slf4j
public class SrStreamLoadClient {
    private final CloseableHttpClient httpClient;
    private final SinkConfig sinkConfig;

    public SrStreamLoadClient(SinkConfig sinkConfig) {
        this.httpClient = HttpClients.createDefault();
        this.sinkConfig = sinkConfig;
    }

    /**
     * 导入数据
     */
    public String streamLoad(String dbTbName, String label, List<TargetDataRow> rows) throws Exception {
        String originUrl = String.format("http://%s:%d/api/%s/%s/_stream_load", sinkConfig.getHost(), sinkConfig.getPort(), sinkConfig.getDbName(), dbTbName);
        String auth = Base64.getEncoder().encodeToString((sinkConfig.getUsername() + ":" + sinkConfig.getPassword()).getBytes(StandardCharsets.UTF_8));

        Header[] headers = {
                new BasicHeader("Authorization", "Basic " + auth),
                new BasicHeader("Expect", "100-continue"),
                new BasicHeader("format", "JSON"),
                // 5GB
                new BasicHeader("load_mem_limit", "5368709120"),
                new BasicHeader("timeout", "1200"),
                new BasicHeader("label", label),
                new BasicHeader("strip_outer_array", "true"),
                new BasicHeader("ignore_json_size", "true"),
                new BasicHeader("jsonpaths", "[ \"$.id\", \"$.record_del_time\", \"$.fb_no\", \"$.data_json\"]")
        };

        List<TargetDataRow.DataRow> dataRows = rows.stream().map(TargetDataRow::getDataRow).collect(Collectors.toList());

        // 构造 Json 数据
        String body = JSON.toJSONString(dataRows);

        // 第一次请求
        HttpPut putReq = buildRequest(originUrl, headers, body);
        try (CloseableHttpResponse response = httpClient.execute(putReq)) {
            if (response.getCode() == HttpStatus.SC_TEMPORARY_REDIRECT) {
                Header locationHeader = response.getFirstHeader("Location");
                if (Objects.isNull(locationHeader)) {
                    throw new RuntimeException("307 redirect received but no Location header");
                }

                // 第二次请求：重新构造 body
                String redirectUrl = locationHeader.getValue();
                HttpPut redirectedReq = buildRequest(redirectUrl, headers, body);
                try (CloseableHttpResponse finalResponse = httpClient.execute(redirectedReq)) {
                    return EntityUtils.toString(finalResponse.getEntity(), StandardCharsets.UTF_8);
                }
            } else {
                return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            }
        }
    }

    private HttpPut buildRequest(String url, Header[] headers, String body) {
        HttpPut putReq = new HttpPut(url);
        putReq.setConfig(RequestConfig.custom()
                .setConnectTimeout(60, TimeUnit.SECONDS)
                .setConnectionRequestTimeout(60, TimeUnit.SECONDS)
                .setResponseTimeout(300, TimeUnit.SECONDS)
                .setMaxRedirects(5)
                .build());

        for (Header h : headers) {
            putReq.setHeader(h);
        }

        putReq.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
        return putReq;
    }

    public void close() throws Exception {
        log.info("Close SrStreamLoadClient...");
        httpClient.close();
    }
}
