package com.example.esdemo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * es配置类
 *
 * @author WangBoran
 * @since 2020/9/1 16:51
 */
@Data
@ConfigurationProperties(prefix = "elastic-search")
@Configuration
public class EsConfig {

    /** ES集群名,默认值 */
    private String clusterName;

    /** ES集群中节点的域名或IP */
    private String hostName;

    /** ES连接端口号 */
    private Integer port;

    /** ES查询的索引名称 */
    private String indices;

    /** type */
    private String type;

}
