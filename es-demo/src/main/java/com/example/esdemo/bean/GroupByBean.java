package com.example.esdemo.bean;

import lombok.Data;
import lombok.ToString;

/**
 * 类描述
 *
 * @author WangBoran
 * @since 2020/9/7 15:52
 */
@Data
@ToString
public class GroupByBean {

    private String lastName;

    private long cityCardinality;

    private long sumBalance;

    private long averageAge;
}
