import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryExcption;

public class Application {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\Users\\hazal\\Desktop\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().appName("streaming-time-op").master("local").getOrCreate();

        Dataset<Row> raw_data = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "8000")
                .option("includeTimestamp", true).load();

        Dataset<Row> products = raw_data.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("product", "timstamp");

        Dataset<Row> resultData = products.groupBy(functions.window(products.col("timestamp"), "1 minute"),
                products.col("product")).count().orderBy("window");

        StreamingQuery start = resultData.writeStream()
                .outputMpde("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        start.awaitTermination();

    }
}
