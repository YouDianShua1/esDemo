package com.example.esdemo.ElasticSearch;

import com.example.esdemo.bean.CustomerEsSelectBean;
import com.example.esdemo.config.EsConfig;
import com.example.esdemo.utils.EsUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * 删除
 *
 * @author WangBoran
 * @since 2020/9/1 17:03
 */
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class EsDeleteService {

    private final RestHighLevelClient restHighLevelClient;

    private final EsConfig esConfig;

    /**
     * 删除索引
     *
     * @param index 索引
     */
    public void deleteIndex(String index){
        DeleteRequest deleteRequest = new DeleteRequest(esConfig.getIndices());
        deleteRequest.index(index);
        try {
            restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据条件进行删除
     *
     * @param condition 条件
     * @param rangeFiled 范围
     * @param from 起始值
     * @param to 终止值
     */
    public long deleteOnCondition(CustomerEsSelectBean condition,String rangeFiled,Object from,Object to){
        //参数为索引名，可以不指定，可以一个，可以多个
        DeleteByQueryRequest request = new DeleteByQueryRequest(esConfig.getIndices());
        // 更新时版本冲突
        request.setConflicts("proceed");
        // 设置查询条件，第一个参数是字段名，第二个参数是字段的值
        request.setQuery(EsUtil.makeRangeBoolQueryBuilder(condition,rangeFiled,from,to));
        // 更新最大文档数
        request.setSize(10);
        // 批次大小
        request.setBatchSize(1000);
        // 并行
        request.setSlices(2);
        // 使用滚动参数来控制“搜索上下文”存活的时间
        request.setScroll(TimeValue.timeValueMinutes(10));
        // 超时
        request.setTimeout(TimeValue.timeValueMinutes(2));
        // 刷新索引
        request.setRefresh(true);
        try {
            BulkByScrollResponse response = restHighLevelClient.deleteByQuery(request, RequestOptions.DEFAULT);
            return response.getStatus().getUpdated();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

}
