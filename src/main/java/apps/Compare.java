package apps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.Tdh;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static datatypes.DataBases.*;
import static java.util.stream.Collectors.toList;
import static utils.SparkUtils.getSparkSession;
import static utils.Tdh.logger_index;


/**
 * args[0] 表名 默认等于0,查询全部的表
 * args[1] 起始时间
 * args[2] 终止时间
 * args[3] 字段名
 */

public class Compare {

    static Logger logger = Logger.getLogger(Compare.class);

    static List<String> abnormalTb = new ArrayList<>();

    static String startTime = "20201008";
    static String endTime = "20201009";
    static String checkTable = "";
    static String noCheckColumn = "";

    public static void main(String[] args) throws SQLException, InterruptedException {

        // 参数检查
        checkArgs(args);
        // 创建sparksession
        SparkSession session = getSparkSession();
        // 初始化要验证的表 -> 写到外头动态配置
        String[] s1 = new String[]{"mid_l2_basi", "mid_l2_basi_clas", "mid_l2_basi_dtl", "mid_l2_basi_tmp", "mid_l2_bond", "mid_l2_bond_curv", "mid_l2_curv", "mid_l2_dpsi", "mid_l2_equi_ns", "mid_l2_index_futr", "mid_l2_indx", "mid_l2_indx_tmp", "mid_l2_lst_trd_cald_tmp", "mid_l2_prd", "mid_l2_prd_ast_clas", "mid_l2_prd_ast_clas_dtl", "mid_l2_prd_ast_clas_tmp", "mid_l2_prd_ast_dtl", "mid_l2_prd_ast_dtl_tmp", "mid_l2_prd_ast_trd_dtl", "mid_l2_prd_dim", "mid_l2_prd_tmp", "mid_l2_prd_trd_assm", "mid_l2_prd_trd_assm_mkt", "mid_l2_prd_trd_assm_mth", "mid_l2_prd_trd_assm_tmp", "mid_l2_prd_trd_assm_view", "mid_l2_prd_trd_repo_pledg", "mid_l2_prd_week", "mid_l2_rat_ns", "mid_l2_repo", "mid_l2_repo_orc", "mid_l2_repo_parquet", "mid_l2_secu_dim", "mid_l2_srp", "mid_l2_stk"};
        List<String> allTables = Arrays.asList(s1);
        // 初始化要检查的table
        List<String> tblist = initCheckTable(args, allTables);
        // 开始检查
        for (String tb : tblist) {
            checkTable(args, session, tb);
        }
        // 检查完毕
        session.stop();
    }

    private static void checkArgs(String[] args) {
        if (args.length == 0) {
            logger.warn(logger_index + "没有输入参数,选择默认参数" + "\n" + startTime + "\n" + endTime + "\n" + checkTable + "\n" + noCheckColumn);
        } else if (args.length == 4) {
            startTime = args[1];
            endTime = args[2];
            checkTable = args[0];
            noCheckColumn = args[3];
        } else {
            throw new RuntimeException("参数输入不合格");
        }
    }


    private static String point = ".";
    private static Dataset<Row> df1;
    private static Dataset<Row> df2;
    private static Dataset<Row> df3;


    private static void checkTable(String[] args, SparkSession session, String tb) throws SQLException {
        String tableSql = selectSql + kbs + point + tb;
        String viewSql = selectSql + defaultDb + point + tb + "_view";
        String replace = betweenAnd.replace("args[1]", startTime).replace("args[2]", endTime);
        String tableCondition = "p_dt " + replace;
        String viewCondition = "t_date" + replace;

        // 得到数据集
        dealData(noCheckColumn, session.sql(tableSql), session.sql(viewSql), tableCondition, viewCondition);
        //开始比对
        compareCDH();


        String[] columns1 = df1.columns();
        StructType schema = df1.schema();
        logger.warn(logger_index + "开始拼接查询TDH的SQL");
        String tdhSelectSql = getTdhSql(columns1, schema).replace("$tbname", tb);
        logger.warn(logger_index + "凭接后tdh查询语句：" + tdhSelectSql);
        ArrayList<Row> rows = Tdh.getData(tdhSelectSql, schema);
        df3 = session.createDataFrame(rows, schema);
        List<String> reduce2 = checkTableHeader(df1, df3);
        String[] columns2 = reduce2.toArray(new String[]{});
        Dataset<Row> tdhData = df3.where(viewCondition).drop("dm_src_info", "task_rs_id").drop(columns2);
        Dataset<Row> cdhData = df1.drop("dm_src_info", "task_rs_id");


//            df3 = df3.na().fill(0);


        Dataset<Row> rowDataset = compare1(tdhData, cdhData);
        if (rowDataset == null || rowDataset.count() == 0) {
            String[] columns3 = rowDataset.columns();
            if (columns3.length != cdhData.columns().length) {
                logger.warn(logger_index + "差异字段列表:" + Arrays.toString(columns3));
            } else {
                logger.warn(logger_index + "比对完成无差异TDH和CDH无差异");
            }
        }
    }

    private static void compareCDH() {
        logger.warn(logger_index + "正在比对CDH数据");
        Dataset<Row> rowDataset1 = compare1(df1, df2);
        if (rowDataset1 == null || rowDataset1.count() == 0) {
            logger.warn(logger_index + "cdh对比完成无差异");
        }
    }

    private static void dealData(String arg, Dataset<Row> sql, Dataset<Row> sql1, String tableCondition, String viewCondition) {
        df1 = sql.drop(arg).where(tableCondition);
        df2 = sql1.drop(arg).where(viewCondition);
        // 防止视图与表结构不一致
        List<String> reduce1 = checkTableHeader(df1, df2);
        // 字段处理：删掉建的视图和标准数据字段不一致的字段.
        String[] columns;
        columns = reduce1.toArray(new String[]{});
        df1 = df1.drop(columns);
        df2 = df2.drop(columns);
    }


    private static List<String> initCheckTable(String[] args, List<String> allTables) {
        List<String> tblist = new ArrayList<>();
        // 判断待检测表是否在范围内。
        if (allTables.contains(checkTable)) {
            tblist.add(checkTable);
        } else if (checkTable.equals("0")) {
            tblist.addAll(allTables);
        } else {
            throw new RuntimeException(checkTable + "该表不在检测范围内");
        }
        return tblist;
    }

    private static String getTdhSql(String[] columns1, StructType schema) {
        // tdh 凭接取数sql
        String tdhSql;
        StructField[] fields = schema.fields();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < columns1.length; i++) {
            if (fields[i].dataType().toString().equals("StringType")) {
                sb.append("cast(" + columns1[i] + " as String ) " + "as " + columns1[i] + ",");
            } else sb.append(columns1[i] + ",");

        }
        String substring = sb.substring(0, sb.length() - 1);
        tdhSql = "select " + substring + " from kbs." + "$tbname" + " ";
        return tdhSql;
    }

    private static Dataset<Row> compare1(Dataset<Row> df1, Dataset<Row> dataFrame) {
        if (df1.count() != dataFrame.count()) {
            throw new RuntimeException(logger_index + "数据量不一致 ....." + "cdh数据量：" + df1.count() + "  tdh数据量：" + dataFrame.count());
        }
        Dataset<Row> result = null;
        String[] columns = df1.columns();
        result = df1.except(dataFrame);

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