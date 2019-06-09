package minimal_algorithms.factory;

import minimal_algorithms.hadoop.RangeTree;
import minimal_algorithms.spark.SparkMinAlgFactory;
import org.apache.spark.sql.SparkSession;

public class MinimalAlgFactory {
    public static SparkMinAlgFactory spark(SparkSession sparkSession, int numPartitions) {
        return new SparkMinAlgFactory(sparkSession, numPartitions);
    }
}
