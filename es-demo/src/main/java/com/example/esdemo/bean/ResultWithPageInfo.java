package com.example.esdemo.bean;

import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * 类描述
 *
 * @author WangBoran
 * @since 2020/9/1 21:50
 */
@Data
@ToString
public class ResultWithPageInfo {

    List<Object> result;

    private int page;

    private int pageSize;

    private long total;
}
