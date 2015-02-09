package net.pascalalma.hadoop.job2;

import net.pascalalma.hadoop.job2.calculate.RankCalculateReduce;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pascal
 */
public class RankCalculateReduceTest {

    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        RankCalculateReduce reducer = new RankCalculateReduce();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> valuesM = new ArrayList<Text>();
        valuesM.add(new Text("A\t1.0\t2"));
        valuesM.add(new Text("M\t1.0\t1"));
        valuesM.add(new Text("|M"));
        valuesM.add(new Text("!"));

        reduceDriver.withInput(new Text("M"), valuesM);

        List<Text> valuesA = new ArrayList<Text>();
        valuesA.add(new Text("Y\t1.0\t2"));
        valuesA.add(new Text("|M,Y"));
        valuesA.add(new Text("!"));

        reduceDriver.withInput(new Text("A"), valuesA);

        List<Text> valuesY = new ArrayList<Text>();
        valuesY.add(new Text("Y\t1.0\t2"));
        valuesY.add(new Text("|Y,A"));
        valuesY.add(new Text("!"));
        valuesY.add(new Text("A\t1.0\t2"));

        reduceDriver.withInput(new Text("Y"), valuesY);

        reduceDriver.withOutput(new Text("A"), new Text("0.6\tM,Y"));
        reduceDriver.withOutput(new Text("M"), new Text("1.4000001\tM"));
        reduceDriver.withOutput(new Text("Y"), new Text("1.0\tY,A"));

        reduceDriver.runTest(false);
    }
}
