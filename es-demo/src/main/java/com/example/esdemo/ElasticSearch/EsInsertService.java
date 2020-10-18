package com.example.esdemo.ElasticSearch;

import com.example.esdemo.bean.CustomerEsBean;
import com.example.esdemo.config.EsConfig;
import com.example.esdemo.utils.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * 新增
 *
 * @author WangBoran
 * @since 2020/9/1 17:03
 */
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class EsInsertService {

    private final RestHighLevelClient restHighLevelClient;

    private final EsConfig esConfig;

    /**
     * @param customers 客户类list
     */
    public void batchInsertCustomers(List<CustomerEsBean> customers){
        for (CustomerEsBean customer : customers) {
            insertCustomer(customer);
        }
    }

    /**
     * @param customer 客户类
     */
    public void insertCustomer(CustomerEsBean customer){
        String customerJson;
        try {
            customerJson = JsonUtil.CustomerEsBeanToJson(customer);
        } catch (JsonProcessingException e) {
            log.error("customer转json出错",e);
            return;
        }
        IndexRequest indexRequest = new IndexRequest(esConfig.getIndices(), esConfig.getType());
        indexRequest.source(customerJson, XContentType.JSON);
        log.info(indexRequest.toString());
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            log.info(response.toString());
        } catch (IOException e) {
            log.error("索引customerJson出错",e);
        }
    }
}
