package ru.chenko.spark.dstream;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class Application {

    private static final String BROKERS = "localhost:9092";

    private static final String INPUT_TOPIC= "spark";

    private static final String OUTPUT_TOPIC= "sparkResult";

    public static void main(String[] args) throws Exception {
        if(args.length < 1) {
            System.out.println("Usage: java -jar jarName byMinutesAggregate");
            return;
        }

        Integer minutes = Integer.parseInt(args[0]);

        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark DStream Application")
                .setMaster("local");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(60*1000));

        SparkSession sparkSession = SparkSession.builder()
                .appName("Spark DStream Application")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        //Схема входных данных (метрик)
        StructType datasetSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("time", DataTypes.TimestampType, false),
                DataTypes.createStructField("value", DataTypes.FloatType, false)
        ));

        //Считываем данные из топика Kafka в датасет в режиме стрима
        Dataset<Row> dataset = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BROKERS)
                .option("subscribe", INPUT_TOPIC)
                .option("includeHeaders", "true")
                .option("failOnDataLoss", "false")
                .option("startingOffsets", "earliest")
                .load();

        //Вычленияем из считанных из Kafka данных интересующие нас данные (избавляемся от вложенности полей)
        Dataset<Row> metricDataset = dataset
                .select(from_json(col("value").cast("string"), datasetSchema))
                .withColumn("metric", col("jsontostructs(CAST(value AS STRING))"))
                .drop("jsontostructs(CAST(value AS STRING))")
                .select(col("metric.id"), col("metric.time"), col("metric.value"));

        //Агрегируем данные по столбцу time и преобразуем результат к исходной структуре
        Dataset<Row> windowDataset = metricDataset
                .withWatermark("time", String.format("%s minutes", minutes))
                .groupBy(col("id"), window(col("time"),String.format("%s minutes", minutes * 2), String.format("%s minutes", minutes)))
                .agg(avg("value").as("value"))
                .withColumn("time", col("window.start"))
                .drop("window")
                .select(col("id"), col("time"), col("value"));

        for (String c: windowDataset.columns()) {
            windowDataset = windowDataset.withColumn(c, windowDataset.col(c).cast(DataTypes.StringType));
        }

        windowDataset.printSchema();

        writeToConsole(windowDataset);
    }

    private static void writeToConsole(Dataset<Row> dataset) throws StreamingQueryException {
        dataset.writeStream()
                .outputMode("complete")
                .format("console")
                .start()
                .awaitTermination();
    }

    // FIXME
    private static void writeToKafka(Dataset<Row> dataset) throws StreamingQueryException {
        dataset
                .writeStream()
                .format("kafka")
                .option("checkpointLocation", "result/checkpoint/")
                .option("kafka.bootstrap.servers", BROKERS)
                .option("topic", OUTPUT_TOPIC)
                .start()
                .awaitTermination();
    }

    // FIXME
    private static void writeToCsv(Dataset<Row> dataset) throws StreamingQueryException {
        dataset
                .coalesce(1)
                .writeStream()
                .format("csv")
                .option("checkpointLocation", "result/checkpoint/")
                .option("path", "result/output/")
                .option("format", "append")
                .outputMode("append")
                .start()
                .awaitTermination();
    }
}
