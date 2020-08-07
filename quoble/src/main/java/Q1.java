import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Q1 {


    public static void main(String... args){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();
        //configure the aws client
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "<YOUR_ACCESS_KEY>"); //fill in real values
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "<YOUR_SECRET_KEY>");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com");

        runQ6(spark);

        spark.stop();
    }
    private static void runQ6(SparkSession spark){
        String path="s3://smile-tcp/Input/lineitem.tbl.*";
        //create schema for the Dataset
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

        //load intput data to the dataset
        Dataset<Row> dataset = spark.read().format("csv").option("delimiter", "|").schema(schema).load(path);
        //create a view for spark sql to work on
        dataset.createOrReplaceTempView("lineitem");
        //sql query Q6
        String query = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-09-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus";
        //run the query
        Dataset<Row> result= spark.sql(query);
        //show results
        result.show();

    }
}
