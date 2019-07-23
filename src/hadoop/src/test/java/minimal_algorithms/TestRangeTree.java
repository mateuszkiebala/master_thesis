package test;

import java.util.List;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.rules.ExpectedException;
import org.apache.avro.AvroRuntimeException;
import minimal_algorithms.hadoop.examples.types.SumStatisticsAggregator;
import minimal_algorithms.hadoop.statistics.StatisticsAggregator;
import minimal_algorithms.hadoop.RangeTree;
import minimal_algorithms.hadoop.utils.Pair;

public class TestRangeTree {

    @org.junit.Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @org.junit.Test
    public void testEmptyTree() {
        RangeTree tree = new RangeTree();
        Assert.assertEquals(0, tree.getBASE());
        Assert.assertArrayEquals(new StatisticsAggregator[0], tree.getNodes());
    }

    @org.junit.Test
    public void testEmptyTreeInsert() throws AvroRuntimeException {
        RangeTree tree = new RangeTree();
        expectedEx.expect(AvroRuntimeException.class);
        expectedEx.expectMessage("Position out of range: 0");
        tree.insert(new SumStatisticsAggregator(10), 0);
    }

    @org.junit.Test
    public void testEmptyTreeInsertNegativePos() throws AvroRuntimeException {
        RangeTree tree = new RangeTree();
        expectedEx.expect(AvroRuntimeException.class);
        expectedEx.expectMessage("Position out of range: -2");
        tree.insert(new SumStatisticsAggregator(10), -2);
    }

    @org.junit.Test
    public void testEmptyTreeQuery() throws AvroRuntimeException {
        RangeTree tree = new RangeTree();
        expectedEx.expect(AvroRuntimeException.class);
        expectedEx.expectMessage("Position (start) out of range: 0");
        tree.query(0, 1);
    }

    @org.junit.Test
    public void testEmptyTreeQueryNegativeStart() throws AvroRuntimeException {
        RangeTree tree = new RangeTree();
        expectedEx.expect(AvroRuntimeException.class);
        expectedEx.expectMessage("Position (start) out of range: -1");
        tree.query(-1, 1);
    }

    @org.junit.Test
    public void testOneNodeTree() {
        List<Pair<StatisticsAggregator, Integer>> elements = new ArrayList<>();
        elements.add(new Pair(new SumStatisticsAggregator(10), 0));

        RangeTree tree = new RangeTree(elements);
        Integer[] expected = {null, 10};
        Assert.assertArrayEquals(expected, unpackSumResult(tree.getNodes()));
    }

    @org.junit.Test
    public void testStartGtEnd() throws AvroRuntimeException {
        expectedEx.expect(AvroRuntimeException.class);
        expectedEx.expectMessage("Start (3) greater than end (2)");

        List<Pair<StatisticsAggregator, Integer>> elements = new ArrayList<>();
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(10), 0));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(30), 1));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(20), 2));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(60), 3));
        RangeTree tree = new RangeTree(elements);

        tree.query(3, 2);
    }

    @org.junit.Test
    public void testPositionOutOfRange() throws AvroRuntimeException {
        expectedEx.expect(AvroRuntimeException.class);
        expectedEx.expectMessage("Position out of range: 4");

        List<Pair<StatisticsAggregator, Integer>> elements = new ArrayList<>();
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(10), 0));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(30), 4));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(20), 2));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(60), 3));
        RangeTree tree = new RangeTree(elements);
    }

    @org.junit.Test
    public void testSum() {
        List<Pair<StatisticsAggregator, Integer>> elements = new ArrayList<>();
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(3), 2));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(2), 1));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(1), 0));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(4), 3));

        RangeTree tree = new RangeTree(elements);
        Integer[] expected = {null, 10, 3, 7, 1, 2, 3, 4};
        Assert.assertArrayEquals(expected, unpackSumResult(tree.getNodes()));
    }

    @org.junit.Test
    public void testSumBaseNotPowerOfTwo() {
        List<Pair<StatisticsAggregator, Integer>> elements = new ArrayList<>();
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(4), 2));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(1), 1));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(3), 0));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(10), 3));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(2), 4));

        RangeTree tree = new RangeTree(elements);
        Integer[] expected = {null, 20, 18, 2, 4, 14, 2, null, 3, 1, 4, 10, 2, null, null, null};
        Assert.assertEquals(8, tree.getBASE());
        Assert.assertArrayEquals(expected, unpackSumResult(tree.getNodes()));
    }

    @org.junit.Test
    public void testQuerySum() {
        List<Pair<StatisticsAggregator, Integer>> elements = new ArrayList<>();
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(4), 2));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(1), 1));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(3), 0));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(10), 3));
        elements.add(new Pair<StatisticsAggregator, Integer>(new SumStatisticsAggregator(2), 4));

        RangeTree tree = new RangeTree(elements);
        Assert.assertEquals(new Integer(3), unpackSumSA(tree.query(0, 0)));
        Assert.assertEquals(new Integer(8), unpackSumSA(tree.query(0, 2)));
        Assert.assertEquals(new Integer(18), unpackSumSA(tree.query(0, 3)));
        Assert.assertEquals(new Integer(20), unpackSumSA(tree.query(0, 4)));
        Assert.assertEquals(new Integer(14), unpackSumSA(tree.query(2, 3)));
        Assert.assertEquals(new Integer(17), unpackSumSA(tree.query(1, 4)));
        Assert.assertEquals(new Integer(16), unpackSumSA(tree.query(2, 5)));
    }

    private Integer[] unpackSumResult(StatisticsAggregator[] nodes) {
        Integer[] result = new Integer[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            StatisticsAggregator sa = nodes[i];
            result[i] = unpackSumSA(sa);
        }
        return result;
    }

    private Integer unpackSumSA(StatisticsAggregator sa) {
        return sa == null ? null : (int) sa.get(0);
    }
}
