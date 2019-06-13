package minimal_algorithms.factory;

import minimal_algorithms.hadoop.HadoopMinAlgFactory;
import minimal_algorithms.hadoop.config.Config;
import minimal_algorithms.spark.SparkMinAlgFactory;
import org.apache.spark.sql.SparkSession;

public class MinimalAlgFactory {
    public static HadoopMinAlgFactory hadoop(Config config) {
        return new HadoopMinAlgFactory(config);
    }

    public static SparkMinAlgFactory spark(SparkSession sparkSession, int numPartitions) {
        return new SparkMinAlgFactory(sparkSession, numPartitions);
    }
}
