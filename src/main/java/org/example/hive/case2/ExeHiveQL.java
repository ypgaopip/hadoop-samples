package org.example.hive.case2;

import java.sql.ResultSet;
import java.sql.SQLException;


public class ExeHiveQL {

    public static void main(String[] args) throws SQLException {
        if (args.length < 2) {
            System.out.print("请输入你要查询的条件：日志级别 日志信息");
            System.exit(1);
        }

        String type = args[0];
        String date = args[1];
        // 在Hive中创建表
        // 注意这个地方time是关键字，使用"`"符号括起来
        HiveUtil.createTable("create table if not exists hadoop_loginfo ( rdate String,`time` ARRAY<string>,type STRING,relateclass STRING,information1 STRING,information2 STRING,information3 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'");
        // 加载Hadoop日志文件，*表示所有的日志文件
        HiveUtil.loadData("load data inpath '/pip/hive-case/2/hadoop/data/*' overwrite into table hadoop_loginfo");
        //查询信息，根据参数
        // 注意这个地方time是关键字，使用"`"符号括起来
        ResultSet res1 = HiveUtil
                .queryHive("select rdate,`time`[0],type,relateclass,information1,information2,information3 from hadoop_loginfo where type='" + type + "' and rdate='" + date + "' ");

        /** 这个地方不能用next输出，如果输出，再后面的保存的时候next就没有值了
         while (res1.next()) {
         System.out.println(res1.getString(1) + "\t" +
         res1.getString(2) + "\t" +
         res1.getString(3) + "\t" +
         res1.getString(4) + "\t" +
         res1.getString(5) + "\t" +
         res1.getString(6) + "\t" +
         res1.getString(7)
         );
         }
         */
        //变换后保存到Mysql中
        HiveUtil.hiveTomysql(res1);
        //关闭Hive连接
        GetConnect.closeHive();
        //关闭Mysql连接
        GetConnect.closemysql();
    }

}
