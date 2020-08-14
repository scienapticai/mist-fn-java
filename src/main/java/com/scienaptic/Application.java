package com.scienaptic;
import static mist.api.jdsl.Jdsl.*;
import mist.api.Handle;
import mist.api.MistFn;
import mist.api.jdsl.JEncoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Application extends MistFn {

    @Override
    public Handle handle() {
        return withArgs(stringArg("inputPath")). //Give complete hdfs path hdfs://<host:port>/filepath for parameter inputPath while running the job
                withMistExtras().
                onSparkSession((filePath, extras, SparkSession) -> {
                    Dataset<Row> df = SparkSession.read()
                            .format("csv")
                            .option("header",true)
                            .load(filePath);
                    String fileName = filePath.substring(0,filePath.lastIndexOf('.'));
                    String outputPath = fileName + ".parquet";
                    df.write().mode("overwrite").parquet(outputPath);
                    return  outputPath;
                }).toHandle(JEncoders.stringEncoder());
    }
}
