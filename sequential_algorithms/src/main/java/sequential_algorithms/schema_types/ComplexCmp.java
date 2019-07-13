package sequential_algorithms.schema_types;

import java.util.Comparator;

public class ComplexCmp implements Comparator<Complex> {
    @Override
    public int compare(Complex o1, Complex o2) {
        return o1.getMiddle().getInner().getInnerInt() > o2.getMiddle().getInner().getInnerInt() ? 1 : (o1.getMiddle().getInner().getInnerInt() < o2.getMiddle().getInner().getInnerInt() ? -1 :
            o1.getIntPrim() > o2.getIntPrim() ? 1 : (o1.getIntPrim() < o2.getIntPrim() ? -1 : 0));
    }
}
