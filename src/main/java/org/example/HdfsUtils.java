package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.util.Date;

public class HdfsUtils {

    /**
     * Exec cmd
     * hadoop jar /hadoop/jar/hadoop-samples-1.0-SNAPSHOT.jar org.example.HdfsUtils
     * <p>
     * Caution: main method cannot throw statement, otherwise cannot be treated as a jar file executed by hadoop
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            System.out.println("============================================================");
            FileSystem fileSystem = getFileSystem();
            System.out.println("File System is " + fileSystem);
            System.out.println("============================================================");
            listDataNodeInfo();
            System.out.println("============================================================");
            checkFileExist("/test3");
            System.out.println("============================================================");
            mkdir("/test3");
            System.out.println("============================================================");
            checkFileExist("/test3");
            System.out.println("============================================================");
            delete("/test3");
            System.out.println("============================================================");
            mkdir("/test3");
            System.out.println("============================================================");
            uploadFile("/opt/kin/tmp/hadoop_temp.txt", "/");
            System.out.println("============================================================");
            downloadFile("/hadoop_temp.txt", "/opt/kin/tmp/hadoop_temp_remote.txt");
            System.out.println("============================================================");
            rename("/hadoop_temp.txt", "/test3/hadoop_temp_new.txt");
            System.out.println("============================================================");
            listDir("/user");
            System.out.println("============================================================");
            filter("/input/*", ".*impossible.*");
            System.out.println("============================================================");
            filter("/input/*", ".*word4.*");
            System.out.println("============================================================");
            getLocation("/test3/hadoop_temp_new.txt");
            System.out.println("============================================================");
            delete("/test3");
            System.out.println("============================================================");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获得文件系统对象
     *
     * @return
     * @throws IOException
     */
    public static FileSystem getFileSystem() throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = null;
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");

        fs = FileSystem.get(conf);

        System.out.println("Connect HDFS successful!");
        return fs;
    }

    /**
     * 列出数据节点名字信息
     *
     * @throws IOException
     */
    public static void listDataNodeInfo() throws IOException {

        FileSystem fs = getFileSystem();
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        String[] nodeNames = new String[dataNodeStats.length];
        System.out.println("List of all the datanodes in the HDFS cluster:");
        for (int i = 0; i < nodeNames.length; i++) {
            nodeNames[i] = dataNodeStats[i].getHostName();
            System.out.println(nodeNames[i]);
        }
        System.out.println("HDFS's URI is " + hdfs.getUri().toString());

        fs.close();

    }

    /**
     * 创建目录
     *
     * @param path
     * @throws IOException
     */
    public static void mkdir(String path) throws IOException {
        FileSystem fs = getFileSystem();
        Path hdfsPath = new Path(path);
        boolean b = fs.mkdirs(hdfsPath);
        if (b) {
            System.out.println("Create dir ok " + path);
        } else {
            System.out.println("Create dir failure " + path);
        }
        fs.close();

    }

    /**
     * 删除文件
     *
     * @param filePath
     * @throws IOException
     */
    public static void delete(String filePath) throws IOException {
        FileSystem fs = getFileSystem();
        Path path = new Path(filePath);
        boolean b = fs.deleteOnExit(path);
        if (b) {
            System.out.println("Delete dir ok " + path);
        } else {
            System.out.println("Delete dir failure " + path);
        }
        fs.close();
    }

    /**
     * 检查文件是否存在
     *
     * @param filePath
     * @throws IOException
     */
    public static void checkFileExist(String filePath) throws IOException {
        FileSystem fs = getFileSystem();
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        Path homeDirectory = hdfs.getHomeDirectory();
        System.out.println("Home directory is " + homeDirectory.toString());
        Path hdfsPath = new Path(filePath);
        boolean exist = fs.exists(hdfsPath);
        if (exist) {
            System.out.println("Exist dir " + filePath);
        } else {
            System.out.println("Not Exist dir " + filePath);
        }
        fs.close();
    }


    /**
     * 上传文件到HDFS
     *
     * @param src
     * @param dest
     * @throws IOException
     */
    public static void uploadFile(String src, String dest) throws IOException {
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
        fs.copyFromLocalFile(false, srcPath, destPath);
        System.out.println("Copy file ok " + src + " to " + dest);
        fs.close();
    }


    /**
     * 丛HDFS下载文件
     *
     * @param remote
     * @param local
     * @throws IOException
     */
    public static void downloadFile(String remote, String local) throws IOException {
        FileSystem fs = getFileSystem();
        Path remotePath = new Path(remote);
        Path localPath = new Path(local);
        fs.copyToLocalFile(false, remotePath, localPath);
        System.out.println("download file ok " + remote + " to " + local);
        fs.close();
    }

    /**
     * 重命名
     *
     * @param oldName
     * @param newName
     * @throws IOException
     */
    public static void rename(String oldName, String newName) throws IOException {
        FileSystem fs = getFileSystem();
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean b = fs.rename(oldPath, newPath);
        if (b) {
            System.out.println("Rename successful " + oldName + " to " + newName);
        } else {
            System.out.println("Rename failure " + oldName + " to " + newName);
        }
        fs.close();
    }

    /**
     * 遍历目录
     *
     * @param dir
     * @throws IOException
     */
    public static void listDir(String dir) throws IOException {
        System.out.println("list dir " + dir + " start");
        FileSystem fs = getFileSystem();
        Path path = new Path(dir);
        listDir(fs, path);
        fs.close();
        System.out.println("list dir " + dir + " end");
    }


    public static void listDir(FileSystem fs, Path path) throws IOException {
        System.out.println(path);
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        FileStatus[] firstLeveFiles = hdfs.listStatus(path);
        if (firstLeveFiles.length > 0) {
            for (FileStatus firstLevelFile : firstLeveFiles) {
                Path firstLevelPath = firstLevelFile.getPath();
                System.out.println(firstLevelPath);
                if (firstLevelFile.isDirectory()) {
                    FileStatus[] nestLevelFiles = hdfs.listStatus(firstLevelPath);
                    if (nestLevelFiles.length > 0) {
                        for (FileStatus nestLevelFile : nestLevelFiles) {
                            Path nestLevelPath = nestLevelFile.getPath();
                            listDir(fs, nestLevelPath);
                        }
                    }
                }
            }
        }
    }

    static class RegexExcludePathFilter implements PathFilter {
        private final String regex;

        public RegexExcludePathFilter(String regex) {
            this.regex = regex;
        }

        @Override
        public boolean accept(Path path) {
            return !path.toString().matches(regex);
        }
    }

    /**
     * 指定路径和过滤条件
     *
     * @param path
     * @param filter
     * @throws IOException
     */
    public static void filter(String path, String filter) throws IOException {
        System.out.println("filter path " + path + " filter " + filter);
        FileSystem fs = getFileSystem();
        Path pathPattern = new Path(path);
        //执行过滤
        FileStatus[] fileStatuses = fs.globStatus(pathPattern, new RegexExcludePathFilter(filter));
        Path[] paths = FileUtil.stat2Paths(fileStatuses);
        for (Path p : paths) {
            System.out.println(p);
        }
        fs.close();
    }

    /**
     * 获得数据块所在位置
     *
     * @param path
     * @throws IOException
     */
    public static void getLocation(String path) throws IOException {
        System.out.println("getLocation path " + path + " start");

        FileSystem fs = getFileSystem();
        Path pathPattern = new Path(path);
        FileStatus fileStatus = fs.getFileStatus(pathPattern);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, pathPattern.depth(), fileStatus.getLen());
        for (BlockLocation fileBlockLocation : fileBlockLocations) {
            String[] hosts = fileBlockLocation.getHosts();
            for (String host : hosts) {
                System.out.println(host);
            }
        }

        //最后修改时间
        long modificationTime = fileStatus.getModificationTime();
        Date date = new Date(modificationTime);
        System.out.println(date);

        fs.close();

    }
}
