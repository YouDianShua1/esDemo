package com.example.esdemo.controller;

import com.example.esdemo.ElasticSearch.EsInsertService;
import com.example.esdemo.Factory.CustomerBeanFactory;
import com.example.esdemo.bean.CustomerEsBean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * es插入demo
 *
 * @author WangBoran
 * @since 2020/9/1 18:18
 */
@RequestMapping("/api/v1/es")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
@RestController
public class EsInsertDemoController {

    private final EsInsertService insertService;

    @GetMapping("/action/insert")
    public void bulkInsertCustomers(){
        List<CustomerEsBean> customers = CustomerBeanFactory.makeCustomerList();
        insertService.batchInsertCustomers(customers);
    }

}
