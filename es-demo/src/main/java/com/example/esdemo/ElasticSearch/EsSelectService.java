package com.example.esdemo.ElasticSearch;

import com.example.esdemo.bean.CustomerEsBean;
import com.example.esdemo.bean.CustomerEsSelectBean;
import com.example.esdemo.bean.GroupByBean;
import com.example.esdemo.bean.ResultWithPageInfo;
import com.example.esdemo.config.EsConfig;
import com.example.esdemo.utils.EsUtil;
import com.example.esdemo.utils.JsonUtil;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 查询
 *
 * @author WangBoran
 * @since 2020/9/1 17:03
 */
@Service
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class EsSelectService {

    private final RestHighLevelClient restHighLevelClient;

    private final EsConfig esConfig;

    /**
     * 条件查询
     */
    public ResultWithPageInfo selectOnConditions(CustomerEsSelectBean condition){
        SearchRequest searchRequest = new SearchRequest(esConfig.getIndices());

        SearchSourceBuilder searchSourceBuilder = EsUtil.makeSourceBuilder(condition);
        searchRequest.source(searchSourceBuilder);
        log.info(searchSourceBuilder.toString());

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.info("查询失败：",e);
            return makeBlankResult(condition);
        }
        return getCustomersFromResponse(condition,searchResponse);
    }

    private ResultWithPageInfo makeBlankResult(CustomerEsSelectBean condition){
        ResultWithPageInfo info = new ResultWithPageInfo();
        info.setTotal(0L);
        info.setPage(condition.getPage());
        info.setPageSize(condition.getPageSize());
        info.setResult(Lists.newArrayList());
        return info;
    }

    private ResultWithPageInfo getCustomersFromResponse(CustomerEsSelectBean condition,SearchResponse searchResponse){
        List<Object> queryResult = new ArrayList<>();
        SearchHits hits;
        if(Objects.nonNull(searchResponse)){
            hits = searchResponse.getHits();
            for (SearchHit hit : hits) {
                Object customer;
                try {
                    customer = JsonUtil.jsonToObj(Object.class,hit.getSourceAsString());
                } catch (IOException e) {
                    log.info("json transfer to object exception");
                    continue;
                }
                queryResult.add(customer);
            }
        }else{
            hits = null;
        }

        //构造结果
        ResultWithPageInfo result = new ResultWithPageInfo();
        result.setResult(queryResult);

        if(Objects.nonNull(hits)){
            result.setTotal(hits.getTotalHits());
        }else{
            result.setTotal(0L);
        }

        result.setPage(condition.getPage());
        result.setPageSize(condition.getPageSize());
        return result;
    }

    /**
     * between and
     */
    public ResultWithPageInfo selectRangeOnCondition(CustomerEsSelectBean condition,String field,Object from,Object to){
        SearchRequest searchRequest = new SearchRequest(esConfig.getIndices());
        SearchSourceBuilder searchSourceBuilder = EsUtil.makeRangeSearchSourceBuilder(condition,field,from,to);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.info("查询失败：",e);
            return makeBlankResult(condition);
        }
        return getCustomersFromResponse(condition,searchResponse);
    }

    /**
     * sum
     */
    public long sumOnConditions(CustomerEsSelectBean condition,String field){
        SearchRequest searchRequest = new SearchRequest(esConfig.getIndices());
        SearchSourceBuilder searchSourceBuilder = EsUtil.makeSumSearchSourceBuilder(condition, field);
        searchRequest.source(searchSourceBuilder);
        log.info(searchSourceBuilder.toString());
        Aggregations aggregations = getAggregations(searchRequest);
        if(Objects.nonNull(aggregations)){
            ParsedSum aggregation = aggregations.get("sum");
            return new Double(aggregation.getValue()).longValue();
        }else{
            return 0L;
        }
    }

    private Aggregations getAggregations(SearchRequest searchRequest){
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.info("查询失败：",e);
            return null;
        }
        return searchResponse.getAggregations();
    }

    /**
     * max
     */
    public long selectMaxOnConditions(CustomerEsSelectBean condition,String field){
        SearchRequest searchRequest = new SearchRequest(esConfig.getIndices());
        SearchSourceBuilder searchSourceBuilder = EsUtil.makeMaxSearchSourceBuilder(condition, field);
        searchRequest.source(searchSourceBuilder);
        log.info(searchSourceBuilder.toString());
        Aggregations aggregations = getAggregations(searchRequest);
        if(Objects.nonNull(aggregations)){
            ParsedSum aggregation = aggregations.get("max");
            double value = aggregation.getValue();
            //注意 不存在时会返回无限
            if (Double.isInfinite(value)) {
                log.info("non-existent，return infinite value");
                return 0L;
            }else{
                return ((Double)value).longValue();
            }
        }else{
            return 0L;
        }
    }

    /**
     * min
     */
    public long selectMinOnConditions(CustomerEsSelectBean condition,String field){
        SearchRequest searchRequest = new SearchRequest(esConfig.getIndices());
        SearchSourceBuilder searchSourceBuilder = EsUtil.makeMinSearchSourceBuilder(condition, field);
        searchRequest.source(searchSourceBuilder);
        log.info(searchSourceBuilder.toString());
        Aggregations aggregations = getAggregations(searchRequest);
        if(Objects.nonNull(aggregations)){
            ParsedSum aggregation = aggregations.get("min");
            return new Double(aggregation.getValue()).longValue();
        }else{
            return 0L;
        }
    }

    /**
     * distinct
     */
    public List<String> distinctFieldOnCondition(CustomerEsSelectBean condition,String field){
        SearchSourceBuilder searchSourceBuilder = EsUtil.makeCollapseSourceBuilder(condition, field);

        SearchRequest searchRequest = new SearchRequest(esConfig.getIndices());
        searchRequest.source(searchSourceBuilder);
        log.info(searchSourceBuilder.toString());

        SearchResponse search = null;
        try {
            search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            return Lists.newArrayList();
        }
        SearchHit[] hits = search.getHits().getHits();
        List<String> distinctFields = Lists.newArrayList();
        for (SearchHit hit : hits) {
            Map<String, DocumentField> fields = hit.getFields();
            String name = fields.get(field).getName();
            distinctFields.add(name);
        }
        return distinctFields;
    }

    /**
     * distinct count
     *
     * @param condition 条件
     * @param field 字段
     * @return long 字段基数
     */
    public long distinctCountFieldOnCondition(CustomerEsSelectBean condition, String field){
        SearchSourceBuilder searchSourceBuilder = EsUtil.makeCardinalitySourceBuilder(condition,field);

        SearchRequest searchRequest = new SearchRequest(esConfig.getIndices());
        searchRequest.source(searchSourceBuilder);
        log.info(searchSourceBuilder.toString());

        SearchResponse search = null;
        try {
            search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            return 0L;
        }
        Aggregations aggregations = search.getAggregations();
        ParsedCardinality aggregation = aggregations.get("distinctCount");
        return aggregation.getValue();
    }

    /**
     * group by
     */
    public List<GroupByBean> groupByField(String field){
        SearchSourceBuilder searchSourceBuilder = EsUtil.makeGroupBySourceBuilder(field);

        SearchRequest searchRequest = new SearchRequest(esConfig.getIndices());

        searchRequest.source(searchSourceBuilder);
        log.info(searchSourceBuilder.toString());

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest,
                    RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        ParsedStringTerms allTermsName = searchResponse.getAggregations().get("groupBy");
        List<? extends Terms.Bucket> buckets = allTermsName.getBuckets();

        List<GroupByBean> list = Lists.newArrayList();

        for (Terms.Bucket bucket : buckets) {

            String key = bucket.getKeyAsString();

            //cityCardinality  sumBalance  averageAge
            ParsedCardinality cityCardinality = bucket.getAggregations().get("cityCardinality");
            ParsedCardinality sumBalance = bucket.getAggregations().get("sumBalance");
            ParsedCardinality averageAge = bucket.getAggregations().get("averageAge");

            GroupByBean groupByBean = new GroupByBean();
            groupByBean.setLastName(key);
            groupByBean.setCityCardinality(cityCardinality.getValue());
            groupByBean.setSumBalance(sumBalance.getValue());
            groupByBean.setAverageAge(averageAge.getValue());

            log.info(groupByBean.toString());
            list.add(groupByBean);
        }

        return list;
    }

}
