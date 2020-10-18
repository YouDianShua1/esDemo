package com.example.esdemo.controller;

import com.example.esdemo.ElasticSearch.EsDeleteService;
import com.example.esdemo.ElasticSearch.EsInsertService;
import com.example.esdemo.bean.CustomerEsSelectBean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 类描述
 *
 * @author WangBoran
 * @since 2020/9/7 17:30
 */

@RequestMapping("/api/v1/es")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
@RestController
public class EsDeleteDemoController {

    private final EsDeleteService deleteService;

    @GetMapping("/action/delete")
    public long delete() {
        //限制城市
        CustomerEsSelectBean condition = new CustomerEsSelectBean();
        condition.setCity("LiaoNing");
        //余额从100到1000的
        String field = "balance";
        Long from = 100L;
        Long to = 1000L;
        //delete
        return deleteService.deleteOnCondition(condition, field, from, to);
    }
}
