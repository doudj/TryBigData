package com.doudou.lesson004.groupComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: GroupMain
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: 启动类
 */
public class GroupMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "group");
        job.setJarByClass(GroupMain.class);

        // 1、读取文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///E:\\大数据\\第三次课1202\\1202_课前资料_mr与yarn\\3、第三天\\9、mapreduce当中的分组求topN\\数据"));

        // 2、自定义map逻辑
        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 3、分区
        job.setPartitionerClass(GroupPartition.class);

        // 4、排序，已经做了
        // 5、combiner 省略
        // 6、分组，自定义分组逻辑
        job.setGroupingComparatorClass(MyGroup.class);

        // 7、设置reducer
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 8、设置输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///E:\\大数据\\第三次课1202\\1202_课前资料_mr与yarn\\3、第三天\\9、mapreduce当中的分组求topN\\数据\\output2"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new GroupMain(), args);
        System.exit(run);
    }
}
