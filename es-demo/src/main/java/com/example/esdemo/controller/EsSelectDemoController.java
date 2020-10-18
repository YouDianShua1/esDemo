package com.example.esdemo.controller;


import com.example.esdemo.ElasticSearch.EsSelectService;
import com.example.esdemo.bean.CustomerEsSelectBean;
import com.example.esdemo.bean.ResultWithPageInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.C;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 查询demo
 *
 * @author WangBoran
 * @since 2020/9/7 15:57
 */
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@RestController
@Slf4j
@RequestMapping("/api/v1/es")
public class EsSelectDemoController {

    private final EsSelectService selectService;

    @GetMapping("/action/getPageInfo")
    public Object getResultWithPageInfo(){
        //限制城市，第二页，每页5条，根据balance降序排列
        CustomerEsSelectBean condition = new CustomerEsSelectBean();
        condition.setOrder(SortOrder.DESC.name());
        condition.setPage(2);
        condition.setPageSize(5);
        condition.setOrderField("balance");
        condition.setCity("LiaoNing");
        //query
        return selectService.selectOnConditions(condition);
    }

    @GetMapping("/action/range")
    public Object selectRangeOnCondition(){
        //限制城市
        CustomerEsSelectBean condition = new CustomerEsSelectBean();
        condition.setCity("LiaoNing");
        //余额从100到1000的
        String field = "balance";
        Long from = 100L;
        Long to = 1000L;
        //query
        return  selectService.selectRangeOnCondition(condition,field,from,to);
    }

    @GetMapping("/action/sum")
    public long sumOnCondition(){
        //限制城市
        CustomerEsSelectBean condition = new CustomerEsSelectBean();
        condition.setCity("LiaoNing");
        //field
        String field = "balance";
        //query
        return selectService.sumOnConditions(condition,field);
    }

    @GetMapping("/action/max")
    public long max(){
        //限制城市
        CustomerEsSelectBean condition = new CustomerEsSelectBean();
        condition.setCity("LiaoNing");
        //field
        String field = "balance";
        //query
        return selectService.selectMaxOnConditions(condition,field);
    }

    @GetMapping("/action/min")
    public long min(){
        //限制城市
        CustomerEsSelectBean condition = new CustomerEsSelectBean();
        condition.setCity("LiaoNing");
        //field
        String field = "balance";
        //query
        return selectService.selectMinOnConditions(condition,field);
    }

    @GetMapping("/action/distinctField")
    public List<String> distinctField(){
        //限制城市
        CustomerEsSelectBean condition = new CustomerEsSelectBean();
        condition.setCity("LiaoNing");
        //field
        String field = "balance";
        //query
        return selectService.distinctFieldOnCondition(condition,field);
    }

    @GetMapping("/action/distinctCount")
    public long distinctCount(){
        //限制城市
        CustomerEsSelectBean condition = new CustomerEsSelectBean();
        condition.setCity("LiaoNing");
        //field
        String field = "lastname";
        //query
        return selectService.distinctCountFieldOnCondition(condition,field);
    }


    @GetMapping("/action/groupBy")
    public Object groupByField(){
        //field
        String field = "lastname";
        //query
        return selectService.groupByField(field);
    }

}
