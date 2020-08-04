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
        return withArgs(stringArg("inputPath")).
                withMistExtras().
                onSparkSession((path, extras, SparkSession) -> {
                    String filePath =  "file://" + path;
                    Dataset<Row> df = SparkSession.read()
                            .format("csv")
                            .option("header",true)
                            .load(filePath);
                    String fileName = filePath.substring(0,path.lastIndexOf('.'));
                    String outputPath = fileName + ".parquet";
                    df.write().mode("overwrite").parquet(outputPath);
                    return  outputPath;
                }).toHandle(JEncoders.stringEncoder());
    }
}
