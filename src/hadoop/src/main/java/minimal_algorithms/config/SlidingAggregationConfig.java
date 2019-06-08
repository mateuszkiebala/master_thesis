package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.utils.Utils;

public class SlidingAggregationConfig extends StatisticsConfig {
    public static final String WINDOW_LENGTH_KEY = "window.length.key";

    public SlidingAggregationConfig(Config config, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema, long windowLength) {
        super(config, cmp, baseSchema, statisticsAggregatorSchema);
        conf.setLong(SlidingAggregationConfig.WINDOW_LENGTH_KEY, windowLength);
    }
}
