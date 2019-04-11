package sortavro.avro_types.statistics;

import org.apache.avro.generic.GenericRecord;

public abstract class Statisticer extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static abstract Statisticer get(GenericRecord record);
    public abstract Statisticer merge(Statisticer that);
}
