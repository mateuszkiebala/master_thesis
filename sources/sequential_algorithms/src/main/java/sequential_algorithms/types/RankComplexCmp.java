package sequential_algorithms.types;

import java.util.Comparator;

public class RankComplexCmp implements Comparator<RankComplex> {
    @Override
    public int compare(RankComplex o1, RankComplex o2) {
        return o1.getRank() > o2.getRank() ? 1 : (o1.getRank() < o2.getRank() ? -1 : new ComplexCmp().compare(o1.getComplex(), o2.getComplex()));
    }
}
