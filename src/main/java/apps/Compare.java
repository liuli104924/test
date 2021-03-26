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

import static java.util.stream.Collectors.toList;
import static utils.SparkUtils.getSparkSession;
import static utils.SparkUtils.getSparkSession2;


/**
 * args[0] 表名 默认等于0,查询全部的表
 * args[1] 起始时间
 * args[2] 终止时间
 * args[3] 字段名
 */

public class Compare {

    static Logger logger = Logger.getLogger(Compare.class);

    public static void main(String[] args) throws SQLException {
        // 创建sparksession
        SparkSession session = getSparkSession();
        session.sql("show databases");

        // 初始化要验证的表 -> 写到外头动态配置
        String[] s1 = new String[]{"mid_l2_basi", "mid_l2_basi_clas", "mid_l2_basi_dtl", "mid_l2_basi_tmp", "mid_l2_bond", "mid_l2_bond_curv", "mid_l2_curv", "mid_l2_dpsi", "mid_l2_equi_ns", "mid_l2_index_futr", "mid_l2_indx", "mid_l2_indx_tmp", "mid_l2_lst_trd_cald_tmp", "mid_l2_prd", "mid_l2_prd_ast_clas", "mid_l2_prd_ast_clas_dtl", "mid_l2_prd_ast_clas_tmp", "mid_l2_prd_ast_dtl", "mid_l2_prd_ast_dtl_tmp", "mid_l2_prd_ast_trd_dtl", "mid_l2_prd_dim", "mid_l2_prd_tmp", "mid_l2_prd_trd_assm", "mid_l2_prd_trd_assm_mkt", "mid_l2_prd_trd_assm_mth", "mid_l2_prd_trd_assm_tmp", "mid_l2_prd_trd_assm_view", "mid_l2_prd_trd_repo_pledg", "mid_l2_prd_week", "mid_l2_rat_ns", "mid_l2_repo", "mid_l2_repo_orc", "mid_l2_repo_parquet", "mid_l2_secu_dim", "mid_l2_srp", "mid_l2_stk"};
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
            Dataset<Row> df1 = session.sql(tableSql).drop(args[3]).where(condition);
            Dataset<Row> df2 = session.sql(viewSql).drop(args[3]).where(viewCondition);
            // 防止视图与表结构不一致
            List<String> reduce1 = checkTableHeader(df1, df2);
            // 如果不一致的表头信息包含待检测的字段 抛异常.
            if (reduce1.contains(args[3])) {
                throw new RuntimeException(tb + ":请检查视图或者表有此字段:" + args[3]);
            }

            String[] columns;
            columns = reduce1.toArray(new String[]{});
            df1 = df1.drop(columns);
            df2 = df2.drop(columns);

            String tdhSql;
            StringBuffer sb = new StringBuffer();

            String[] columns1 = df1.columns();
            StructType schema = df1.schema();

            for (String s : columns1) {
                sb.append(s + ",");
            }
            String substring = sb.substring(0, sb.length() - 1);
            tdhSql = "select " + substring + " from kbs." + tb + " ";
            System.out.println(tdhSql);
            ArrayList<Row> rows = Tdh.getData(tdhSql, schema);
            Dataset<Row> dataFrame = session.createDataFrame(rows, schema);
//            dataFrame = dataFrame.na().fill(0);

            Dataset<Row> where = dataFrame.where("basi_code='2070.0' ").where(viewCondition).drop("dm_src_info","task_rs_id").select("t_date","basi_code","basi_name","curr_code","t_pont","pont_rat","t1_pont","basi_yld_d7","basi_yld_m1","basi_yld_m3","basi_yld_m6","basi_yld_y1","basi_yld_setp_d");

            Dataset<Row> where2 = df1.where("basi_code='2070.0' ").drop("dm_src_info","task_rs_id").select("t_date","basi_code","basi_name","curr_code","t_pont","pont_rat","t1_pont","basi_yld_d7","basi_yld_m1","basi_yld_m3","basi_yld_m6","basi_yld_y1","basi_yld_setp_d");
            System.out.println("tdh-----------------------------------:");
            where.show(5);



            System.out.println("cdh-----------------------------------:");
            where2.show(5);
            extracted(tb,where,where2);

        }

        session.stop();
    }


    private static void extracted(String tb, Dataset<Row> df1, Dataset<Row> df2) {
        // 比较差异
        Dataset<Row> except = df1.except(df2);
        if (except.count() != 0) {
            except.show(5);
            throw new RuntimeException("检测到" + ": " + tb + "表存在差异" + " 差异条数共" + except.count() + "条");
        } else {
            logger.warn(tb + "：检测通过");
        }
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