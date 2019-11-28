package com.doudouj.lesson001;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: IOFileTest
 * @Author: doudou
 * @Datetime: 2019/11/28
 * @Description: hdfs文件的io处理
 */
public class HdfsIOFileTest {

    //上传文件
    @Test
    public void putFileToHdfs() throws URISyntaxException, IOException {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        // 2、创建输入流
        FileInputStream fis = new FileInputStream(new File("E:\\大数据\\hdfs第一次课后资料\\settings-user.xml"));
        // 3、获取输出流
        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://node01:8020/doudou/dir1/setting-user.xml"));
        // 4、流拷贝
        IOUtils.copyBytes(fis, fos, 1024);
        // 5、关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

    //下载文件
    @Test
    public void downloadFileFromHdfs() throws IOException, URISyntaxException {
        // 1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        // 2、创建输入流
        FSDataInputStream fis = fileSystem.open(new Path("hdfs://node01:8020/doudou/dir1/setting-user.xml"));
        // 3、获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\大数据\\hdfs第一次课后资料\\settings-user01.xml"));
        // 4、流拷贝
        IOUtils.copyBytes(fis, fos, 1024);
        // 5、关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }
}
