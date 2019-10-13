package prefix_app;

import java.util.Comparator;
import org.apache.hadoop.io.Text;

public class IndexedStatistics {
  public static final String DELIMITER = "%";
  public int key;
  public long statistics;
  public String encodedValue;

  public IndexedStatistics(String key, long statistics) {
    this.encodedValue = key + DELIMITER + Long.toString(statistics);
  }

  public IndexedStatistics(Text encodedValue) {
    String[] tokens = encodedValue.toString().split(DELIMITER);
    this.key = Integer.parseInt(tokens[0]);
    this.statistics = Long.parseLong(tokens[1]);
  }

  public Text toText() {
    return new Text(this.encodedValue);
  }

  public static class IndexedStatisticsComparator implements Comparator<IndexedStatistics> {
    @Override
    public int compare(IndexedStatistics o1, IndexedStatistics o2) {
      return o1.key > o2.key ? 1 : (o1.key < o2.key ? -1 :
          (o1.statistics > o2.statistics ? 1 : o1.statistics < o2.statistics ? -1 : 0));
    }
  }

  public static Comparator<IndexedStatistics> cmp = new IndexedStatisticsComparator();
}
