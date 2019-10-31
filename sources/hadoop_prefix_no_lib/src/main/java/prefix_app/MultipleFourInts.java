package prefix_app;

import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;

public class MultipleFourInts {
  public static final String DELIMITER = "#";
  private List<FourInts> fourIntsSeq;

  public MultipleFourInts(Text encodedValues){
    this(encodedValues.toString());
  }

  public MultipleFourInts(String encodedValues) {
    this.fourIntsSeq = new ArrayList<>();
    for (String value : encodedValues.split(DELIMITER)) {
      this.fourIntsSeq.add(new FourInts(value));
    }
  }

  public MultipleFourInts(List<FourInts> fourIntsSeq) {
    this.fourIntsSeq = fourIntsSeq;
  }

  public List<FourInts> getValues() {
    return this.fourIntsSeq;
  }

  @Override
  public String toString() {
    List<String> result = new ArrayList();
    for (FourInts fourInts : this.fourIntsSeq) {
      result.add(fourInts.toString());
    }
    return String.join(DELIMITER, result);
  }

  public Text toText() {
    return new Text(this.toString());
  }
}
