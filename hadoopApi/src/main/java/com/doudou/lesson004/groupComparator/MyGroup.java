package com.doudou.lesson004.groupComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: MyGroup
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: 自定义分组类
 */
public class MyGroup extends WritableComparator {

    /**
     * 构造器，通过反射，构造OrderBean对象
     * 接收到的k2，是orderBean类型，需要告诉分组，以orderBean接收参数
     */
    public MyGroup() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
        //以orderId作为比较条件，判断哪些orderId相同作为一组
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
