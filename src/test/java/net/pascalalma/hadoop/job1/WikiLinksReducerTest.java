package net.pascalalma.hadoop.job1;

import net.pascalalma.hadoop.job1.parse.WikiLinksReducer;
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
public class WikiLinksReducerTest {

    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        WikiLinksReducer reducer = new WikiLinksReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> valuesA = new ArrayList<Text>();
        valuesA.add(new Text("M"));
        valuesA.add(new Text("Y"));
       reduceDriver.withInput(new Text("A"), valuesA);
       reduceDriver.withOutput(new Text("A"), new Text("1.0\tM,Y"));

       reduceDriver.runTest();
    }
}
