package utils;

import apps.Compare;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;

public class Tdh {

    //Hive2 Driver class name

    // org.apache.hive.jdbc.HiveDriver
    private static String driverName = "io.transwarp.jdbc.InceptorDriver";
    static Logger logger = Logger.getLogger(Compare.class);
    static String logger_index = "--------------------------------:";


    public static void main(String[] args) throws SQLException {

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


