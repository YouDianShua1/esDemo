package com.example.esdemo.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 类描述
 *
 * @author WangBoran
 * @since 2020/9/1 18:11
 */
@Configuration
public class EsClientConfig {

    private final EsConfig esConfig;

    @Autowired
    public EsClientConfig(EsConfig esConfig) {
        this.esConfig = esConfig;
    }

    /**
     * @return 封装 RestClient
     */
    @Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient(){
        return new RestHighLevelClient(RestClient.builder(new HttpHost(esConfig.getHostName(), esConfig.getPort(), "http")));
    }
}
