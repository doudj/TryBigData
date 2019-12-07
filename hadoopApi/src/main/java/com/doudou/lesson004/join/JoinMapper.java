package com.doudou.lesson004.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName: ReduceJoinMapper
 * @Author: doudou
 * @Datetime: 2019/12/7
 * @Description: 数据关联处理，自定义mapper
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    //现在我们读取了两个文件，如何确定当前处理的这一行数据是来自哪个文件里面的
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 通过文件名判断，获取文件的切片
//        FileSplit inputSplit = (FileSplit) context.getInputSplit();//获取输入的文件切片
//        String name = inputSplit.getPath().getName();
//        if (name.equals("orders.txt")) {
//            //订单表数据
//        } else {
//            //商品表数据
//        }
        String[] splits = value.toString().split(",");
        if (value.toString().startsWith("p")) {
            //p开头的数据是商品数据，以商品id为k2，相同商品的数据会放到一起
            context.write(new Text(splits[0]), value);
        } else {
            context.write(new Text(splits[2]), value);
        }
    }
}
