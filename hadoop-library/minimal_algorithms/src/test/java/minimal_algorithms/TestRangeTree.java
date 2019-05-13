package test;

import java.util.List;
import java.util.ArrayList;
import javafx.util.Pair;
import org.junit.Assert;
import minimal_algorithms.examples.types.SumStatisticsAggregator;
import minimal_algorithms.statistics.StatisticsAggregator;
import minimal_algorithms.RangeTree;

public class TestRangeTree {

    @org.junit.Test
    public void testOneNodeTree() {
        List<Pair<StatisticsAggregator, Integer>> elements = new ArrayList<>();
        elements.add(new Pair(new SumStatisticsAggregator(10), 0));

        RangeTree tree = new RangeTree(elements);
        Integer[] expected = {null, 10};
        Assert.assertArrayEquals(expected, unpackSumResult(tree.getNodes()));
    }

    @org.junit.Test(expected = org.apache.avro.AvroRuntimeException.class)
    public void testPositionOutOfRange() {
        List<Pair<StatisticsAggregator, Integer>> elements = new ArrayList<>();
        elements.add(new Pair(new SumStatisticsAggregator(10), 0));
        elements.add(new Pair(new SumStatisticsAggregator(30), 4));
        elements.add(new Pair(new SumStatisticsAggregator(20), 2));
        elements.add(new Pair(new SumStatisticsAggregator(60), 3));
        RangeTree tree = new RangeTree(elements);
    }

    private Integer[] unpackSumResult(StatisticsAggregator[] nodes) {
        Integer[] result = new Integer[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            StatisticsAggregator s = nodes[i];
            result[i] = s == null ? null : (int) s.get(0);
        }
        return result;
    }
}
