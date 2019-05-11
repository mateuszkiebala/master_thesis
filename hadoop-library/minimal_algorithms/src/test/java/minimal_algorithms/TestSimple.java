import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import minimal_algorithms.record.Record4Float;
import minimal_algorithms.avro_types.statistics.*;
import minimal_algorithms.avro_types.terasort.*;
import minimal_algorithms.avro_types.utils.*;
import org.apache.avro.generic.GenericRecord;
import minimal_algorithms.PhasePrefix.PrefixMapper;
import minimal_algorithms.config.BaseConfig;
import minimal_algorithms.config.StatisticsConfig;
import minimal_algorithms.config.GroupByConfig;
import minimal_algorithms.Utils;
import minimal_algorithms.MyOutputCollector;
import minimal_algorithms.MyReporter;


public class TestSimple {

    Logger logger = LoggerFactory.getLogger(TestSimple.class);
    private MapDriver<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<SendWrapper>> mapDriver;

    @Before
    public void setUp() throws IOException {
        PrefixMapper mapper = new PrefixMapper();
        mapDriver = new MapDriver<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<SendWrapper>>();
        mapDriver.setMapper(mapper);

        MultipleMainObjects.setSchema(Record4Float.getClassSchema());
        SendWrapper.setSchema(Record4Float.getClassSchema(), SumStatisticsAggregator.getClassSchema());

        Configuration conf = mapDriver.getConfiguration();
        AvroSerialization.addToConfiguration(conf);
        AvroSerialization.setKeyWriterSchema(conf, Schema.create(Schema.Type.INT));
        AvroSerialization.setValueWriterSchema(conf, MultipleMainObjects.getClassSchema());

        Job job = new Job(conf);
        job.setMapperClass(PrefixMapper.class);
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleMainObjects.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, SendWrapper.getClassSchema());

        conf.setInt("sampling.noOfSplits", 1);
        Utils.storeSchemaInConf(conf, Record4Float.getClassSchema(), BaseConfig.BASE_SCHEMA);
        BaseConfig baseConfig = new BaseConfig(conf, Record4Float.getClassSchema());
        StatisticsConfig statsConfig = new StatisticsConfig(conf, Record4Float.getClassSchema(), SumStatisticsAggregator.getClassSchema());
    }

    @Test
    public void testMap() throws IOException {
        AvroKey<Integer> avKey = new AvroKey<>();
        AvroValue<MultipleMainObjects> avVal = new AvroValue<>();

        List<GenericRecord> values = new ArrayList<>();
        values.add(new Record4Float(1.0f, 2.0f, 3.0f, 4.0f));
        avVal.datum(new MultipleMainObjects(values));
        avKey.datum(1);
        mapDriver.setInputKey(avKey);
        mapDriver.setInputValue(avVal);

        final List<Pair<AvroKey<Integer>, AvroValue<MultipleMainObjects>>> inputs = new ArrayList<>();
        inputs.add(new Pair<AvroKey<Integer>, AvroValue<MultipleMainObjects>>(mapDriver.getInputKey(), mapDriver.getInputValue()));

        final InputSplit inputSplit = new MyInputSplit();

        try {
            final MockMapContextWrapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<SendWrapper>> wrapper = new MockMapContextWrapper<>(
                    inputs, mapDriver.getCounters(), mapDriver.getConfiguration(), inputSplit);

            final Mapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<SendWrapper>>.Context context = wrapper.getMockContext();
            myMapper.run(context);
            return wrapper.getOutputs();
        } catch (final InterruptedException ie) {
            throw new IOException(ie);
        }

        //MyOutputCollector<AvroKey<Integer>, AvroValue<SendWrapper>> outputCollector = new MyOutputCollector<>(Schema.create(Schema.Type.INT), SendWrapper.getClassSchema());
        //MyReporter reporter = new MyReporter(MyReporter.ReporterType.Mapper, mapDriver.getCounters());
        //mapDriver.map(mapDriver.getInputKey(), mapDriver.getInputValue(), outputCollector, reporter);

        //mapDriver.map(mapDriver.getInputKey(), mapDriver.getInputValue())
        //final List<Pair<AvroKey<Integer>, AvroValue<SendWrapper>>> result = mapDriver.run();
    }
}
