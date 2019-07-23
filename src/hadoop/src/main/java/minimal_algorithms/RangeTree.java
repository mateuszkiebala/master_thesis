package minimal_algorithms.hadoop;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;
import minimal_algorithms.hadoop.statistics.StatisticsAggregator;
import minimal_algorithms.hadoop.utils.Pair;

public class RangeTree extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    public static void setSchema(Schema schema) {
        SCHEMA$ = SchemaBuilder
                .record("RangeTree").namespace("minimal_algorithms.hadoop")
                .fields()
                .name("base").type().intType().noDefault()
                .name("nodes").type().array().items(schema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return BASE;
            case 1: return nodes;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: BASE = (Integer)value$; break;
            case 1: nodes = (StatisticsAggregator[])value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public static RangeTree deepCopy(RangeTree tree) {
        return SpecificData.get().deepCopy(getClassSchema(), tree);
    }

    public static RangeTree deepCopy(Schema treeSchema, RangeTree tree) {
        return SpecificData.get().deepCopy(treeSchema, tree);
    }

    private int BASE;
    private StatisticsAggregator[] nodes;

    public RangeTree() {
        BASE = 0;
        nodes = new StatisticsAggregator[0];
    }

    public RangeTree(int elementsNo) {
        BASE = computeBASE(elementsNo);
        nodes = new StatisticsAggregator[2 * BASE];
    }

    public RangeTree(List<Pair<StatisticsAggregator, Integer>> elements) {
        BASE = computeBASE(elements.size());
        nodes = new StatisticsAggregator[2 * BASE];
        for (Pair<StatisticsAggregator, Integer> element : elements) {
            insert(element.getKey(), element.getValue());
        }
    }

    public void insert(StatisticsAggregator element, int start) {
        if (start < 0 || start >= BASE)
            throw new org.apache.avro.AvroRuntimeException("Position out of range: " + start);

        int pos = BASE + start;
        nodes[pos] = StatisticsAggregator.safeMerge(nodes[pos], element);
        while (pos != 1) {
            pos = pos / 2;
            nodes[pos] = StatisticsAggregator.safeMerge(nodes[2 * pos], nodes[2 * pos + 1]);
        }
    }

    public StatisticsAggregator query(int start, int end) {
        if (start > end)
            throw new org.apache.avro.AvroRuntimeException("Start (" + start + ") greater than end (" + end + ")");

        if (start < 0 || start >= BASE)
            throw new org.apache.avro.AvroRuntimeException("Position (start) out of range: " + start);

        if (end < 0 || end >= BASE)
            throw new org.apache.avro.AvroRuntimeException("Position (end) out of range: " + end);

        int vs = BASE + start;
        int ve = BASE + end;
        StatisticsAggregator result = nodes[vs];
        if (vs != ve) result = StatisticsAggregator.safeMerge(result, nodes[ve]);

        while (vs / 2 != ve / 2) {
            if (vs % 2 == 0) result = StatisticsAggregator.safeMerge(result, nodes[vs + 1]);
            if (ve % 2 == 1) result = StatisticsAggregator.safeMerge(result, nodes[ve - 1]);
            vs /= 2;
            ve /= 2;
        }
        return result;
    }

    public StatisticsAggregator[] getNodes() {
        return nodes;
    }

    public int getBASE() {
        return BASE;
    }

    private int computeBASE(int n) {
        return n <= 0 ? 0 : (int) Math.pow(2.0, Math.ceil(log2(((double) n))));
    }

    private double log2(double x) {
        return Math.log10(x) / Math.log10(2.0);
    }
}
