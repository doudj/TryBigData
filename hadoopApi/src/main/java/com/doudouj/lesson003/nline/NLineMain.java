package com.doudouj.lesson003.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: NLineMain
 * @Author: doudou
 * @Datetime: 2019/12/3
 * @Description: TODO
 */
public class NLineMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new NLineMain(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "nline");
        job.setJarByClass(NLineMain.class);
        NLineInputFormat.setNumLinesPerSplit(job,3);
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job,new Path("file:///E:\\大数据\\第三次课1202\\1202_课前资料_mr与yarn\\3、第三天\\3、NLineInputFormat\\数据"));
        //第二步：自定义mapper类
        job.setMapperClass(NLineMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //第三步到第六步  分区，排序，规约，分组
        job.setReducerClass(NLineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///E:\\大数据\\第三次课1202\\1202_课前资料_mr与yarn\\3、第三天\\3、NLineInputFormat\\数据\\output"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }
}
