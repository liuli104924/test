package apps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import utils.Tdh;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;

import static java.util.stream.Collectors.toList;
import static utils.SparkUtils.getSparkSession;


/**
 * args[0] 表名 默认等于0,查询全部的表
 * args[1] 起始时间
 * args[2] 终止时间
 * args[3] 字段名
 */

public class Compare {

    static Logger logger = Logger.getLogger(Compare.class);

    static List<String> abnormalTb = new ArrayList<>();


    public static void main(String[] args) throws SQLException, InterruptedException {
        // 创建sparksession
        SparkSession session = getSparkSession();
        // 初始化要验证的表 -> 写到外头动态配置
        String[] s1 = new String[]{"mid_l2_stk"};
        List<String> allTables = Arrays.asList(s1);
        List<String> tblist = new ArrayList<>();
        // 判断待检测表是否在范围内。
        if (allTables.contains(args[0])) {
            tblist.add(args[0]);
        } else if (args[0].equals("0")) {
            tblist.addAll(allTables);
        } else {
            throw new RuntimeException(args[0] + "该表不在检测范围内");
        }

        // 拼接查询sql
        for (String tb : tblist) {
            String tableSql = "select * from " + "xc_kbs." + tb;
            String viewSql = "select * from " + "default." + tb + "_view";
            String condition = "p_dt between " + args[1] + " and " + args[2];
            String viewCondition = "t_date between " + args[1] + " and " + args[2];

            // 得到数据集
            Dataset<Row> df1 = session.sql(tableSql).drop(args[3]).where(condition);
            Dataset<Row> df2 = session.sql(viewSql).drop(args[3]).where(viewCondition);

            // 防止视图与表结构不一致
            List<String> reduce1 = checkTableHeader(df1, df2);
            // 如果不一致的表头信息包含待检测的字段 抛异常.
            if (reduce1.contains(args[3])) {
                throw new RuntimeException(tb + ":请检查视图或者表有此字段:" + args[3]);
            }

            // 删掉建的视图和标准数据字段不一致的字段.
            String[] columns;
            columns = reduce1.toArray(new String[]{});
            df1 = df1.drop(columns);

            //开始比对
            String[] columns1 = df1.columns();
            df2 = df2.drop(columns);
            Dataset<Row> rowDataset1 = compare1(df1, df2);
            if (rowDataset1 == null || rowDataset1.count() == 0) {
                logger.warn("cdh对比完成无差异");
            }


            // tdh 凭接取数sql
            String tdhSql;
            StringBuffer sb = new StringBuffer();
            StructType schema = df1.schema();
            for (String s : columns1) {
                sb.append(s + ",");
            }

            String substring = sb.substring(0, sb.length() - 1);
            tdhSql = "select " + substring + " from kbs." + tb + " ";
            logger.warn("tdh查询语句：" + tdhSql);

            ArrayList<Row> rows = Tdh.getData(tdhSql, schema);
            Dataset<Row> dataFrame = session.createDataFrame(rows, schema);

            List<String> reduce2 = checkTableHeader(df1, dataFrame);

            String[] columns2 = reduce2.toArray(new String[]{});


//            dataFrame = dataFrame.na().fill(0);
            Dataset<Row> where = dataFrame.where(viewCondition).drop("dm_src_info", "task_rs_id").drop(columns2);
            Dataset<Row> where2 = df1.drop("dm_src_info", "task_rs_id");

            where2.cache();
            where.cache();

            System.out.println("tdh-----------------------------------:");
            where.show(5);
            System.out.println("cdh-----------------------------------:");
            where2.show(5);


            Dataset<Row> rowDataset = compare1(where, where2);
            if (rowDataset == null || rowDataset.count() == 0) {
                String[] columns3 = rowDataset.columns();
                if (columns3.length != where2.columns().length) {
                    logger.warn("比对完成无差异TDH和CDH无差异");
                } else {
                    logger.warn("差异字段列表:" + Arrays.toString(columns3));
                }
            }
        }

        session.stop();
    }

    private static Dataset<Row> compare1(Dataset<Row> df1, Dataset<Row> dataFrame) {
        if (df1.count() != dataFrame.count()) {
            throw new RuntimeException("数据量不一致");
        }
        Dataset<Row> result = null;
        String[] columns = df1.columns();
        result = df1.except(dataFrame);
        result.show(5);
        if (result.count() != 0) {
            columns = Arrays.copyOfRange(columns, 0, columns.length / 2);
            ArrayList<String> clist = new ArrayList<>(Arrays.asList(columns));
            clist.remove("secu_id");
            clist.remove("prd_code");
            String[] s1 = new String[clist.size()];
            clist.toArray(s1);
            System.out.println("clist.size():" + clist.size());
            df1 = df1.drop(s1);
            dataFrame = dataFrame.drop(s1);
            result = compare1(df1, dataFrame);
        } else {
            return result;
        }
        return result;
    }


    private static boolean compare(Dataset<Row> df1, Dataset<Row> df2) {
        // 比较差异
        Dataset<Row> except = df1.except(df2);
        if (except.count() != 0) {
            except.show();
            String[] columns = df1.columns();
            Dataset<Row> drop = df1.drop(columns);
            Dataset<Row> drop2 = df2.drop(columns);
            compare(drop, drop2);
            return false;
        } else return true;
    }


    /**
     * @param df1
     * @param df2
     * @return List<String> 返回两个数据集不同的表头信息。
     */
    private static List<String> checkTableHeader(Dataset<Row> df1, Dataset<Row> df2) {
        List<String> l1 = Arrays.asList(df1.columns());
        List<String> l2 = Arrays.asList(df2.columns());
        List<String> reduce2 = l1.stream().filter(item -> !l2.contains(item)).collect(toList());
        List<String> reduce1 = l2.stream().filter(item -> !l1.contains(item)).collect(toList());
        reduce1.addAll(reduce2);
        return reduce1;
    }
}


//        List<StructField> structFilelds = new ArrayList<StructField>();
//        structFilelds.add(DataTypes.createStructField("id", LongType, true));
//        structFilelds.add(DataTypes.createStructField("name", DataTypes.LongType, true));
//        StructType structType = DataTypes.createStructType(structFilelds);