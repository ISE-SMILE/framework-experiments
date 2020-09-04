
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Q6 {


    public static void main(String... args){
        SparkSession spark = SparkSession
                .builder()
                .appName("TPCQ6")
                .getOrCreate();
                

        runQ6(spark);

        spark.stop();
    }
    private static void runQ6(SparkSession spark){
        String path="s3://smile-tcp/Input/lineitem.tbl.*";
        StructType schema=DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("L_ORDERKEY",  DataTypes.IntegerType, true),
                DataTypes.createStructField("L_PARTKEY", DataTypes.IntegerType, true),
                DataTypes.createStructField("L_SUPPKEY", DataTypes.IntegerType, true),
                DataTypes.createStructField("L_LINENUMBER", DataTypes.IntegerType, true),
                DataTypes.createStructField("L_QUANTITY", DataTypes.DoubleType, true),
                DataTypes.createStructField("L_EXTENDEDPRICE", DataTypes.DoubleType, true),
                DataTypes.createStructField("L_DISCOUNT", DataTypes.DoubleType, true),
                DataTypes.createStructField("L_TAX", DataTypes.DoubleType, true),
                DataTypes.createStructField("L_RETURNFLAG", DataTypes.StringType, true),
                DataTypes.createStructField("L_LINESTATUS", DataTypes.StringType, true),
                DataTypes.createStructField("L_SHIPDATE", DataTypes.DateType, true),
                DataTypes.createStructField("L_COMMITDATE", DataTypes.DateType, true),
                DataTypes.createStructField("L_RECEIPTDATE", DataTypes.DateType, true),
                DataTypes.createStructField("L_SHIPINSTRUCT", DataTypes.StringType, true),
                DataTypes.createStructField("L_SHIPMODE", DataTypes.StringType, true),
                DataTypes.createStructField("L_COMMENT", DataTypes.StringType, true),

        });
        Dataset<Row> dataset = spark.read().format("csv").option("delimiter", "|").schema(schema).load(path);// spark.read().parquet("s3://smile-tcp/Input/lineitem.parquet");

        //Dataset<Row> frame= sqlContext.createDataFrame(stringRDD,Row.class);
        //Dataset<Row> ds= frame.as(Encoders.bean(Row.class));



        // dataset.show(2);
        Calendar instance = Calendar.getInstance();
        instance.set(1994,Calendar.JANUARY,0);
        Date datel = instance.getTime();//instance.getTimeInMillis() * 1000L;
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
        instance.set(1995, Calendar.JANUARY,0);
        long dateh = instance.getTimeInMillis() * 1000L;
        dataset.createOrReplaceTempView("lineitem");
        String query = String.format("select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >=   %1$s and l_shipdate < %4$s and l_discount between %2$.2f -0.01 and %2$.2f + 0.01 and l_quantity < %3$d", "'1994-01-01'", 0.05, 25, "'1995-01-01'" );
//        String query = "select l_shipdate from lineitem limit 1";
        System.out.println(query);
        Dataset<Row> result= spark.sql(query);
        result.write().csv("s3://smile-tcp/baseline/q6.csv");

    }
}
