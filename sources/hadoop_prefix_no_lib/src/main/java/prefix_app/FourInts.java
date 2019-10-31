package prefix_app;

import java.util.Comparator;
import org.apache.hadoop.io.Text;

public class FourInts {
  public static final String DELIMITER = " ";
  private int first;
  private int second;
  private int third;
  private int fourth;
  private String encodedValues;

  public FourInts(String encodedValues) {
    this.encodedValues = encodedValues;
    String[] values = encodedValues.split(DELIMITER);
    this.first = Integer.parseInt(values[0]);
    this.second = Integer.parseInt(values[1]);
    this.third = Integer.parseInt(values[2]);
    this.fourth = Integer.parseInt(values[3]);
  }

  public FourInts(Text encodedValues) {
    this(encodedValues.toString());
  }

  public long getValue() {
    return this.second + this.third + this.fourth;
  }

  @Override
  public String toString() {
    return this.encodedValues;
  }

  public Text toText() {
    return new Text(this.toString());
  }

  public static class FourIntsComparator implements Comparator<FourInts> {
    @Override
    public int compare(FourInts o1, FourInts o2) {
      return o1.first > o2.first ? 1 : (o1.first < o2.first ? -1 : 0);
    }
  }

  public static Comparator<FourInts> cmp = new FourIntsComparator();
}
