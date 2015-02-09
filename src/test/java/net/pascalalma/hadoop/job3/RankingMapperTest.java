package net.pascalalma.hadoop.job3;

import net.pascalalma.hadoop.job3.result.RankingMapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: pascal
 */
public class RankingMapperTest {

    MapDriver<LongWritable, Text, FloatWritable, Text> mapDriver;

    @Before
    public void setUp() {
        RankingMapper mapper = new RankingMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("A\t0.454545\tM,Y"));
        mapDriver.withInput(new LongWritable(2), new Text("M\t1.90\tM"));
        mapDriver.withInput(new LongWritable(3), new Text("Y\t0.68898\tY,A"));


        //PLease note that we cannot check for ordering here because that is done by Hadoop after the Map phase
        mapDriver.withOutput(new FloatWritable(0.454545f), new Text("A"));
        mapDriver.withOutput(new FloatWritable(1.9f), new Text("M"));
        mapDriver.withOutput(new FloatWritable(0.68898f), new Text("Y"));
        mapDriver.runTest(false);
    }
}
