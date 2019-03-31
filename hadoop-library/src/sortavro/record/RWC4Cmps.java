package sortavro.record;

import java.util.Comparator;

/**
 *
 * @author jsroka
 */
public class RWC4Cmps {

    public static class FirstComparator implements Comparator<Record4Float> {
        @Override
        public int compare(Record4Float o1, Record4Float o2) {
            return o1.first > o2.first ? 1 : (o1.first < o2.first ? -1 :
                    o1.second > o2.second ? 1 : (o1.second < o2.second ? -1 :
                            o1.third > o2.third ? 1 : (o1.third < o2.third ? -1 :
                                    o1.fourth > o2.fourth ? 1 : (o1.fourth < o2.fourth ? -1 : 0))));
        }
    }
    
    public static class SecondComparator implements Comparator<Record4Float> {
        @Override
        public int compare(Record4Float o1, Record4Float o2) {
            return o1.second > o2.second ? 1 : (o1.second < o2.second ? -1 :
                    o1.third > o2.third ? 1 : (o1.third < o2.third ? -1 :
                            o1.fourth > o2.fourth ? 1 : (o1.fourth < o2.fourth ? -1 :
                                    o1.first > o2.first ? 1 : (o1.first < o2.first ? -1 : 0))));
        }
    }
    
    public static class ThirdComparator implements Comparator<Record4Float> {
        @Override
        public int compare(Record4Float o1, Record4Float o2) {
            return o1.third > o2.third ? 1 : (o1.third < o2.third ? -1 :
                    o1.fourth > o2.fourth ? 1 : (o1.fourth < o2.fourth ? -1 :
                            o1.first > o2.first ? 1 : (o1.first < o2.first ? -1 :
                                    o1.second > o2.second ? 1 : (o1.second < o2.second ? -1 : 0))));
        }
    }
    
    public static class FourthComparator implements Comparator<Record4Float> {
        @Override
        public int compare(Record4Float o1, Record4Float o2) {
            return o1.fourth > o2.fourth ? 1 : (o1.fourth < o2.fourth ? -1 :
                    o1.first > o2.first ? 1 : (o1.first < o2.first ? -1 :
                            o1.second > o2.second ? 1 : (o1.second < o2.second ? -1 :
                                    o1.third > o2.third ? 1 : (o1.third < o2.third ? -1 : 0))));
        }
    }
        
    public static Comparator<Record4Float> firstCmp = new FirstComparator();
    public static Comparator<Record4Float> secondCmp = new SecondComparator();
    public static Comparator<Record4Float> thirdCmp = new ThirdComparator();
    public static Comparator<Record4Float> fourthCmp = new FourthComparator();
}
