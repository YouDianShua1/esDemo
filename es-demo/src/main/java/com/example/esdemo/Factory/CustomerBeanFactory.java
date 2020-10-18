package com.example.esdemo.Factory;

import com.example.esdemo.bean.CustomerEsBean;

import java.util.ArrayList;
import java.util.List;

/**
 * customer工厂类
 *
 * @author WangBoran
 * @since 2020/9/1 17:51
 */
public class CustomerBeanFactory {

    public static List<CustomerEsBean> makeCustomerList() {
        List<CustomerEsBean> list = new ArrayList<>();

        CustomerEsBean bean1 = new CustomerEsBean("2",100,"LeBron","James",30,"M","address","NBA","email","city",
                "state");

        CustomerEsBean bean2 = new CustomerEsBean("3",200,"Kobe","Bryant",25,"M","address","NBA","email","city",
                "state");

        CustomerEsBean bean3 = new CustomerEsBean("4",300,"Stephen","Curry",40,"M","address","NBA","email","city",
                "state");

        CustomerEsBean bean4 = new CustomerEsBean("5",400,"James","Harden",45,"M","address","NBA","email","city",
                "state");
        list.add(bean1);
        list.add(bean2);
        list.add(bean3);
        list.add(bean4);
        return list;

    }

}
