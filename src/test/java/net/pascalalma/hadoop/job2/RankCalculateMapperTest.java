package net.pascalalma.hadoop.job2;

import net.pascalalma.hadoop.job2.calculate.RankCalculateMapper;
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
public class RankCalculateMapperTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;

    @Before
    public void setUp() {
        RankCalculateMapper mapper = new RankCalculateMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("A\t1.0\tM,Y"));
        mapDriver.withInput(new LongWritable(2), new Text("M\t1.0\tM"));
        mapDriver.withInput(new LongWritable(3), new Text("Y\t1.0\tY,A"));
        mapDriver.withOutput(new Text("M"), new Text("A\t1.0\t2"));
        mapDriver.withOutput(new Text("A"), new Text("Y\t1.0\t2"));
        mapDriver.withOutput(new Text("Y"), new Text("A\t1.0\t2"));
        mapDriver.withOutput(new Text("A"), new Text("|M,Y"));
        mapDriver.withOutput(new Text("M"), new Text("M\t1.0\t1"));
        mapDriver.withOutput(new Text("Y"), new Text("Y\t1.0\t2"));
        mapDriver.withOutput(new Text("A"), new Text("!"));
        mapDriver.withOutput(new Text("M"), new Text("|M"));
        mapDriver.withOutput(new Text("M"), new Text("!"));
        mapDriver.withOutput(new Text("Y"), new Text("|Y,A"));
        mapDriver.withOutput(new Text("Y"), new Text("!"));
        mapDriver.runTest(false);
    }
}
