package prefix_app;

import java.io.IOException;
import java.util.Comparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PrefixApp extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(PrefixApp.class);

    private int computeRatioForRandom(int itemsNo, int partitionsNo) {
      int itemsPerPartition = 1 + itemsNo / partitionsNo;
      double rho = 1. / itemsPerPartition * Math.log(((double) itemsNo) * partitionsNo);
      return (int) (1 / rho);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: PrefixApp <home_dir> <input_path> <output_path> <items_no> <partitions_no> <reduce_tasks_no>");
            return -1;
        }

        Configuration conf = getConf();
        int itemsNo = Integer.parseInt(args[3]);
        int partitionsNo = Integer.parseInt(args[4]);
        int reducersNo = Integer.parseInt(args[5]);
        conf.setInt(Utils.RATIO_KEY, computeRatioForRandom(itemsNo, partitionsNo));
        conf.setInt(Utils.SPLIT_POINTS_KEY, partitionsNo);
        conf.setInt(Utils.REDUCERS_NO_KEY, reducersNo);
        Path input = new Path(args[1]);
        //Sampling.run(input, new Path("sampling"), conf);
        //Sorting.run(input, new Path(args[0] + "/sampling"), new Path("sorting"), conf);
        //PartitionStatistics.run(new Path(args[0] + "/sorting"), new Path("part_stats"), conf);
        return Prefix.run(new Path(args[0] + "/sorting"), new Path(args[0] + "/part_stats"), new Path("prefix"), conf);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PrefixApp(), args);
        System.exit(res);
    }
}
