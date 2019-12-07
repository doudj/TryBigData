package com.doudou.lesson003.myOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: MyOutputFormatMain
 * @Author: doudou
 * @Datetime: 2019/12/7
 * @Description: 自定义输入路径 启动类
 */
public class MyOutputFormatMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), MyOutputFormatMain.class.getSimpleName());
        job.setJarByClass(MyOutputFormatMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///E:\\大数据\\1206-课后资料-mapreduce\\10、自定义outputFormat\\数据\\input"));
        job.setMapperClass(MyOutputFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(MyOutputFormat.class);
        MyOutputFormat.setOutputPath(job, new Path("file:///E:\\大数据\\1206-课后资料-mapreduce\\10、自定义outputFormat\\数据\\output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new MyOutputFormatMain(), args);
        System.exit(run);
    }
}
