package com.example.esdemo.bean;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 类描述
 *
 * @author WangBoran
 * @since 2020/9/1 18:48
 */
@EqualsAndHashCode(callSuper = true)
@Data
@ToString
public class CustomerEsSelectBean extends CustomerEsBean{

    private String OrderField;

    private String order;

    private int page;

    private int pageSize;


}
