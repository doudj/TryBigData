package com.doudou.lesson003.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: WordCount01
 * @Author: doudou
 * @Datetime: 2019/12/2
 * @Description: 单词统计，这个类作为mapreduce程序的入口类
 * 计算过程中，需要实现Tool接口
 */
public class WordCount01 extends Configured implements Tool {

    /**
     * 实现Tool接口之后，实现run方法
     * 这个run方法用于组装我们程序的逻辑，即组装8个步骤
     * @param strings
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] strings) throws Exception {
        // 获取Job对象，组装8个步骤，每一个步骤都是一个类
        Configuration conf = super.getConf();
        // 本地模式
//        conf.set("mapreduce.framework.name", "local");
//        conf.set("yarn.resourcemanager.hostname", "local");

        Job job = Job.getInstance(conf, "mrdemo01");
        // 实际工作中，程序运行完成之后一般都是打包到集群上面去运行，打成一个jar包
        // 如果要打包到集群上面去运行，必须添加下面一行配置
        job.setJarByClass(WordCount01.class);

        //第一步：读取文件，解析成key-value对，k1：偏移量，v1：一行文本内容
        job.setInputFormatClass(TextInputFormat.class);
        //制定我们去哪一个路径读取文件
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/doudou/dir1/input"));
        //第二步：自定义map逻辑，接收k1-v1转换成k2-v2输出
        job.setMapperClass(MyMapper.class);
        // 设置map阶段输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 第三步~第六步：分区，排序，规约，分组，这里不用，省略

        // 第七步：自定义reduce逻辑
        job.setReducerClass(MyReducer.class);
        // 设置key3，value3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 第八步：输出k3-v3进行保存
        job.setOutputFormatClass(TextOutputFormat.class);
        //一定要注意：输出路径是需要不存在的，如果存在就报错
        TextOutputFormat.setOutputPath(job, new Path("hdfs://node01:8020/doudou/dir1/output"));

        // 提交job任务
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("hello", "world");
        //提交run方法后，得到一个程序的退出状态码
        int run = ToolRunner.run(configuration, new WordCount01(), args);
        // 根据退出状态码，退出整个进程
        System.exit(run);
    }
}
