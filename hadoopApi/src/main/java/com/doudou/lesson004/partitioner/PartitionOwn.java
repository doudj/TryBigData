package com.doudou.lesson004.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: PartitionOwn
 * @Author: doudou
 * @Datetime: 2019/12/4
 * @Description: 分区
 */
public class PartitionOwn extends Partitioner<Text, FlowBean> {
    // 重写getPartition方法，自定义分区
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phoneNo = text.toString();
        if (null != phoneNo && !"".equals(phoneNo)) {
            if (phoneNo.startsWith("135")) {
                return 0;
            } else if (phoneNo.startsWith("136")) {
                return 1;
            } else if (phoneNo.startsWith("137")) {
                return 2;
            } else if (phoneNo.startsWith("138")) {
                return 3;
            } else if (phoneNo.startsWith("139")) {
                return 4;
            } else {
                return 5;
            }
        } else {
            return 5;
        }
    }
}
