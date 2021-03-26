package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;


public class SparkUtils {

    public static SparkSession getSparkSession2()  {

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkConf conf = new SparkConf();
        conf.setAppName("compare");
        conf.setMaster("yarn");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
        return session;
    }





    public static SparkSession getSparkSession()  {
        System.setProperty("java.security.krb5.conf", "D:/doc/k8s/krb5.conf");
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkConf conf = new SparkConf();
        conf.setAppName("hiveTest");
        conf.setMaster("local[*]");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        Configuration conf1 = new Configuration();
        conf1.set("hadoop.security.authentication", "Kerberos");
        conf1.set("hadoop.security.authorization", "true");
        conf1.set("keytab.file", "D:/doc/k8s/xunce.keytab");
        conf1.set("Kerberos.principal", "hive@XUNCE.COM");
        UserGroupInformation.setConfiguration(conf1);
        try {
            UserGroupInformation.loginUserFromKeytab("hive@XUNCE.COM", "D:/doc/k8s/xunce.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return session;
    }
}
