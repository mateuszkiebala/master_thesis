package minimal_algorithms.hadoop.examples.types;

import java.util.Comparator;

public class FourIntsCmp implements Comparator<FourInts> {
  @Override
  public int compare(FourInts o1, FourInts o2) {
    return o1.first > o2.first ? 1 : (o1.first < o2.first ? -1 : 0);
  }
}
