package com.doudouj.lesson001;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: FirstTest
 * @Author: doudou
 * @Datetime: 2019/11/27
 * @Description: hdfs的文件处理
 */

public class HdfsFileTest {

//    1、创建文件夹
    @Test
    public void mkdirToHdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        boolean mk = fileSystem.mkdirs(new Path("/doudou/dir1/input"));
        System.out.println("mk : " + mk);
        fileSystem.close();
    }
//2、文件上传
    @Test
    public void uploadFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(new Path("file:///E:\\大数据\\第三次课1202\\1202_课前资料_mr与yarn\\3、第三天\\1、wordCount_input\\数据\\2.txt"),new Path("hdfs://node01:8020/doudou/dir1/input"));
        fileSystem.close();
    }
//3、文件下载
    @Test
    public void downloadFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
//        fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/doudou/dir1/settings.xml"),new Path("file:///E:\\大数据\\hdfs第一次课后资料\\settings-user.xml"));
        fileSystem.copyToLocalFile(false, new Path("hdfs://node01:8020/doudou/dir1/settings.xml"),new Path("file:///E:\\大数据\\hdfs第一次课后资料\\settings-user.xml"), true);
        fileSystem.close();
    }
//4、自主完成hdfs文件删除操作
    @Test
    public void deleteFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path("hdfs://node01:8020/doudou/dir1/input"), false);
        fileSystem.close();
    }

//5、自主完成hdfs文件重命名操作
    @Test
    public void renameFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.rename(new Path("hdfs://node01:8020/doudou/dir1/settings-user.xml"), new Path("hdfs://node01:8020/doudou/dir1/settings-my.xml"));
        fileSystem.close();
    }

//6、查看hdfs文件相信信息
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        // 3 关闭资源
        fs.close();
    }

}
