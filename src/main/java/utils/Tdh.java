package utils;

import apps.Compare;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import  org.apache.hive.service.cli.thrift.TTypeDesc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;

public class Tdh {

    //Hive2 Driver class name

    // org.apache.hive.jdbc.HiveDriver
    private static String driverName = "io.transwarp.jdbc.InceptorDriver";
    static Logger logger = Logger.getLogger(Compare.class);
    public static String logger_index = "--------------------------------:";


    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        //Hive2 JDBC URL with LDAP
        String jdbcURL = "jdbc:hive2://192.168.20.91:10000/default";
        String user = "cmsindicators";
        String password = "cmsindicators";
        Connection conn = DriverManager.getConnection(jdbcURL, user, password);
        Statement stmt = conn.createStatement();


        ResultSet rs = stmt.executeQuery("select \n" +
                "\n" +
                "\n" +
                "\n" +
                "amp_day,ann_vix_y1_d,rpt_rat,roe,eps,lga_unit_yld_rat,t_date,secu_id,exch_code,secu_name,typ_code,typ_codeii,curr_code,prev_cls_prc,open_prc,clg_rw,high_prc,low_prc,ant_unit_nval,tnv_vol,iss_prc,unit_bons,act_pla_rat,bonus_rat,pb,pe,shr_circ,ttl_shr_aft,beta_hsi,beta_spx,beta_djia,beta_ixic,forc_cnv_dt,beta_ssec,beta_smei,beta_sbi,beta_gemi,lst_sect_code,lst_sect_name,tnv_rat_trd_20d,indu_code_fir,tnv_val,ant_unit_nval_week,ant_unit_nval_mth,ant_unit_nval_qut,ant_unit_nval_year,forc_cnv_week,forc_cnv_mth,forc_cnv_qut,forc_cnv_year,tnv_val_week,tnv_val_mth,tnv_val_qut,tnv_val_year,alpha_120d,beta_120d,avg_tnv_rat_5d,avg_tnv_rat_10d,tnv_vol_5d,tnv_vol_10d,amp_day,ann_vix_y1_d,rpt_rat,roe,eps,lga_unit_yld_rat from kbs.mid_l2_stk");
        ResultSetMetaData rsmd = rs.getMetaData();
    }


    public static ArrayList<Row> getData(String sql, StructType schema) throws SQLException {

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        //Hive2 JDBC URL with LDAP
        String jdbcURL = "jdbc:hive2://192.168.20.91:10000/default";
        String user = "cmsindicators";
        String password = "cmsindicators";
        Connection conn = DriverManager.getConnection(jdbcURL, user, password);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData rsmd = rs.getMetaData();

        int size = rsmd.getColumnCount();
        logger.warn(logger_index + "比较字段共" + size + "列");
        ArrayList<Row> rows = getRows(schema, rs);
        rs.close();
        stmt.close();
        conn.close();
        return rows;
    }


    private static ArrayList<Row> getRows(StructType schema, ResultSet data1) throws SQLException {

        ArrayList<Row> rows = new ArrayList<>();
        while (data1.next()) {
            StructField[] fields = schema.fields();
            Object[] a = new Object[fields.length];
            for (int i = 0; i < schema.length(); i++) {
                StructField field = fields[i];
                DataType x = field.dataType();
                String name = field.name();
                String value = data1.getString(name);
                Object o = typeConver(value, x, name);
                a[i] = o;
            }
            Row row = RowFactory.create(a);
            rows.add(row);
        }
        return rows;
    }


    private static Object typeConver(String value, DataType type, String name) throws SQLException {
        String typeToString = type.toString();
        if (value == null) {
            return null;
        }
        if (typeToString.equals("LongType")) {
            return Long.valueOf(value);
        } else if (typeToString.equals("StringType")) {
            return value;
        } else if (typeToString.equals("DecimalType(32,16)")||typeToString.equals("DecimalType(10,0)")) {
//            return Decimal.apply(value);
            return BigDecimal.valueOf(Double.valueOf(value));
        } else if (typeToString.equals("TimestampType")) {
            return Timestamp.valueOf(value);
        } else {
            throw new RuntimeException(":" + type + ":类型未识别");
        }
    }



}


