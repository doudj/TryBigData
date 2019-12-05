package com.doudou.lesson004.groupComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: GroupPartition
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: 自定义分区类
 */
public class GroupPartition extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
