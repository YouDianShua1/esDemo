package com.example.esdemo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * 银行账户es类
 *
 * @author WangBoran
 * @since 2020/9/1 17:23
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class CustomerEsBean {

    private String accountNumber;
    private long balance;
    private String firstName;
    private String lastName;
    private long age;
    private String gender;
    private String address;
    private String employer;
    private String email;
    private String city;
    private String state;
}
