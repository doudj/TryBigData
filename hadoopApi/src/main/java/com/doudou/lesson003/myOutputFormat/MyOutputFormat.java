package com.doudou.lesson003.myOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * @ClassName: MyOutputFormat
 * @Author: doudou
 * @Datetime: 2019/12/6
 * @Description: TODO
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        Path goodComment = new Path("file:///E:\\大数据\\1206-课后资料-mapreduce\\10、自定义outputFormat\\数据\\output\\goodCommnet\\good.txt");
        Path badComment = new Path("file:///E:\\大数据\\1206-课后资料-mapreduce\\10、自定义outputFormat\\数据\\output\\badComment\\bad.txt");
        FSDataOutputStream goodStream = fileSystem.create(goodComment);
        FSDataOutputStream badStream = fileSystem.create(badComment);
        return new MyRecordWriter(goodStream, badStream);
    }

    static class MyRecordWriter extends RecordWriter<Text, NullWritable> {
        FSDataOutputStream goodStream;
        FSDataOutputStream badStream;

        public MyRecordWriter(FSDataOutputStream goodStream, FSDataOutputStream badStream) {
            this.goodStream = goodStream;
            this.badStream = badStream;
        }

        @Override
        public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
            if (key.toString().split("\t")[9].equals("0")) {
                //0 表示好评
                goodStream.write(key.toString().getBytes());
                goodStream.write("\r\n".getBytes());
            } else {
                badStream.write(key.toString().getBytes());
                badStream.write(key.toString().getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (badStream != null) badStream.close();
            if (goodStream != null) goodStream.close();
        }
    }

}
