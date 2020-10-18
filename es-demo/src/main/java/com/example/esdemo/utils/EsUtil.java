package com.example.esdemo.utils;

import com.example.esdemo.bean.CustomerEsSelectBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Objects;


/**
 * ES util
 *
 * @author WangBoran
 * @since 2020/9/1 20:59
 */
@Slf4j
public class EsUtil {

    /**
     * 根据customer生成SearchSourceBuilder
     *
     * @param condition 查询条件
     * @return BoolQueryBuilder 查询对象
     */
    public static SearchSourceBuilder makeSourceBuilder(CustomerEsSelectBean condition){
        SearchSourceBuilder searchSourceBuilder = makeSearchSourceBuilder(condition);
        BoolQueryBuilder boolQueryBuilder = makeBoolQueryBuilder(condition);
        return searchSourceBuilder.query(boolQueryBuilder);
    }

    /**
     * SearchSourceBuilder包括排序与分页
     *
     * @param condition 筛选条件
     * @return SearchSourceBuilder 查询对象
     */
    static private SearchSourceBuilder makeSearchSourceBuilder(CustomerEsSelectBean condition){
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().trackTotalHits(true);

        if(StringUtils.isNotBlank(condition.getOrderField())){
            FieldSortBuilder fieldSortBuilder;
            if(!StringUtils.equalsIgnoreCase(SortOrder.DESC.name(),condition.getOrder())){
                fieldSortBuilder = new FieldSortBuilder(condition.getOrderField()).order(SortOrder.ASC);
            }else{
                fieldSortBuilder = new FieldSortBuilder(condition.getOrderField()).order(SortOrder.DESC);
            }
            sourceBuilder.sort(fieldSortBuilder);
        }

        if(condition.getPage()!=0){
            if(condition.getPageSize()==0){
                condition.setPageSize(10);
            }
            //from 是指从第多少条开始
            sourceBuilder.from((condition.getPage() - 1) * condition.getPageSize());
            sourceBuilder.size(condition.getPageSize());
        }
        return sourceBuilder;

    }

    /**
     * BoolQueryBuilder 筛选
     *
     * @param condition 筛选条件
     * @return BoolQueryBuilder
     */
    static public BoolQueryBuilder makeBoolQueryBuilder(CustomerEsSelectBean condition){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if(StringUtils.isNotBlank(condition.getAccountNumber())){
            TermQueryBuilder accountNumberQueryBuilder = QueryBuilders.termQuery("account_number",
                    condition.getAccountNumber());
            boolQueryBuilder.must(accountNumberQueryBuilder);
        }

        if(StringUtils.isNotBlank(condition.getCity())){
            TermQueryBuilder cityQueryBuilder = QueryBuilders.termQuery("city",
                    condition.getCity());
            boolQueryBuilder.must(cityQueryBuilder);
        }

        if(condition.getBalance()!=0L){
            TermQueryBuilder balanceQueryBuilder = QueryBuilders.termQuery("balance",
                    condition.getBalance());
            boolQueryBuilder.must(balanceQueryBuilder);
        }

        return boolQueryBuilder;
    }

    /**
     * 根据customer和字段范围生成SearchSourceBuilder
     *
     * @param condition 条件
     * @param field 字段
     * @param from 开始
     * @param to 结束
     * @return SearchSourceBuilder 查询对象
     */
    static public SearchSourceBuilder makeRangeSearchSourceBuilder(CustomerEsSelectBean condition,
                                                                   String field,
                                                                   Object from,
                                                                   Object to){
        SearchSourceBuilder sourceBuilder = makeSearchSourceBuilder(condition);
        BoolQueryBuilder boolQueryBuilder = makeRangeBoolQueryBuilder(condition, field, from, to);
        sourceBuilder.query(boolQueryBuilder);
        return sourceBuilder;
    }

    /**
     * 包括筛选条件，与范围筛选
     *
     * @param condition 筛选条件
     * @param field 范围过滤字段
     * @param from 起始值
     * @param to 末值
     * @return BoolQueryBuilder boolQueryBuilder查询对象
     */
    static public BoolQueryBuilder makeRangeBoolQueryBuilder(CustomerEsSelectBean condition,
                                                              String field,
                                                              Object from,
                                                              Object to){
        BoolQueryBuilder boolQueryBuilder = makeBoolQueryBuilder(condition);
        if(StringUtils.isNotBlank(field)&& Objects.nonNull(from)&&Objects.nonNull(to)){
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field).from(from).to(to);
            boolQueryBuilder.must(rangeQueryBuilder);
        }
        return boolQueryBuilder;
    }

    /**
     * 创建条件sum查询
     *
     * @param condition 筛选条件
     * @param field sum字段
     * @return SearchSourceBuilder 查询对象
     */
    static public SearchSourceBuilder makeSumSearchSourceBuilder(CustomerEsSelectBean condition,String field){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().trackTotalHits(true);
        BoolQueryBuilder boolQueryBuilder = makeBoolQueryBuilder(condition);
        SumAggregationBuilder sumBuilder = AggregationBuilders.sum("sum").field(field);
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.aggregation(sumBuilder);
        return searchSourceBuilder;
    }

    /**
     * 创建条件max查询
     *
     * @param condition 筛选条件
     * @param field max字段
     * @return SearchSourceBuilder 查询对象
     */
    static public SearchSourceBuilder makeMaxSearchSourceBuilder(CustomerEsSelectBean condition,String field){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().trackTotalHits(true);
        BoolQueryBuilder boolQueryBuilder = makeBoolQueryBuilder(condition);
        MaxAggregationBuilder maxBuilder = AggregationBuilders.max("max").field(field);
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.aggregation(maxBuilder);
        return searchSourceBuilder;
    }

    /**
     * 创建条件min查询
     *
     * @param condition 筛选条件
     * @param field max字段
     * @return SearchSourceBuilder 查询对象
     */
    static public SearchSourceBuilder makeMinSearchSourceBuilder(CustomerEsSelectBean condition,String field){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().trackTotalHits(true);
        BoolQueryBuilder boolQueryBuilder = makeBoolQueryBuilder(condition);
        MinAggregationBuilder minBuilder = AggregationBuilders.min("min").field(field);
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.aggregation(minBuilder);
        return searchSourceBuilder;
    }


    /**
     * 条件distinct
     *
     * @param condition 条件
     * @param field distinct字段
     * @return SearchSourceBuilder 查询对象
     */
    static public SearchSourceBuilder makeCollapseSourceBuilder(CustomerEsSelectBean condition, String field){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().trackTotalHits(true);
        BoolQueryBuilder boolQueryBuilder = makeBoolQueryBuilder(condition);
        CollapseBuilder collapseBuilder = new CollapseBuilder(field);
        return searchSourceBuilder.query(boolQueryBuilder).collapse(collapseBuilder);
    }


    /**
     * distinct count 字段
     *
     * @param condition 条件
     * @param field 字段名
     * @return SearchSourceBuilder 查询对象
     */
    static public SearchSourceBuilder makeCardinalitySourceBuilder(CustomerEsSelectBean condition, String field){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().trackTotalHits(true);
        BoolQueryBuilder boolQueryBuilder = makeBoolQueryBuilder(condition);
        CardinalityAggregationBuilder distinctCountAggregation =
                AggregationBuilders.cardinality("distinctCount").field(field);
        return searchSourceBuilder.query(boolQueryBuilder).aggregation(distinctCountAggregation);
    }

    /**
     * group by example
     * group by 姓
     *
     * @return  SearchSourceBuilder 查询对象
     */
    static public SearchSourceBuilder makeGroupBySourceBuilder(String field){
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //根据姓氏分组
        TermsAggregationBuilder groupBy = AggregationBuilders.terms("groupBy")
                .field(field).size(Integer.MAX_VALUE);
        //城市个数
        AggregationBuilder total = AggregationBuilders.cardinality("cityCardinality")
                .field("city");
        //余额总数
        SumAggregationBuilder totalTimes = AggregationBuilders.sum("sumBalance")
                .field("balance");
        //平均年龄
        AvgAggregationBuilder avgUsedTimes = AggregationBuilders
                .avg("averageAge").field("age");
        groupBy.subAggregation(total);
        groupBy.subAggregation(totalTimes);
        groupBy.subAggregation(avgUsedTimes);

        return sourceBuilder.aggregation(groupBy);
    }
}
