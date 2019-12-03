package com.doudouj.lesson003.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: MyMapper
 * @Author: doudou
 * @Datetime: 2019/12/2
 * @Description: 自定义Mapper类需要继承mapper，有四个泛型
 * keyin： key1 行偏移量 Long
 * valuein： v1 一行文本内容 String
 * keyout： key2 每一个单词 String
 * valueout： v2 1 int
 * 在hadoop中没有沿用java中的基本数据类型，使用自己封装的一套基本数据类型
 * long ==> LongWritable
 * String ==> Text
 * int ==> IntWritable
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 继承mapper之后，复写map方法，每次读取一行数据，都会来调用一下map方法
     * @param key
     * @param value
     * @param context 上下文对象，承上启下，将上面步骤发过来的数据，通过context将数据发送到下面的步骤里面去
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取第一行数据
        String line = value.toString();
        String[] lineWordsArr = line.split(",");
        Text text = new Text();
        IntWritable intWritable = new IntWritable(1);
        for (String word : lineWordsArr) {
            //将每个单词出现都记做一次
            text.set(word);
            context.write(text, intWritable);
        }
    }
}
