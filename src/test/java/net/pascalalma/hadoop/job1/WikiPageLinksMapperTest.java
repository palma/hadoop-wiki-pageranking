package net.pascalalma.hadoop.job1;

import net.pascalalma.hadoop.job1.parse.WikiPageLinksMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: pascal
 */
public class WikiPageLinksMapperTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;

    String testPageA = " <page>\n" +
            "    <title>A</title>\n" +
            "    <ns>0</ns>\n" +
            "    <id>121173</id>\n" +
            "    <revision>\n" +
            "      <id>593475724</id>\n" +
            "      <parentid>593439792</parentid>\n" +
            "      <model>wikitext</model>\n" +
            "      <format>text/x-wiki</format>\n" +
            "      <text xml:space=\"preserve\" bytes=\"6523\">[[Y]] [[M]]</text>\n" +
            "    </revision>";

    String testPageY = " <page>\n" +
            "    <title>Y</title>\n" +
            "    <ns>0</ns>\n" +
            "    <id>121173</id>\n" +
            "    <revision>\n" +
            "      <id>593475724</id>\n" +
            "      <parentid>593439792</parentid>\n" +
            "      <model>wikitext</model>\n" +
            "      <format>text/x-wiki</format>\n" +
            "      <text xml:space=\"preserve\" bytes=\"6523\">[[A]] [[Y]]</text>\n" +
            "    </revision>\n" +
            "  </page>";
    String testPageM = " <page>\n" +
            "    <title>M</title>\n" +
            "    <ns>0</ns>\n" +
            "    <id>121173</id>\n" +
            "    <revision>\n" +
            "      <id>593475724</id>\n" +
            "      <parentid>593439792</parentid>\n" +
            "      <model>wikitext</model>\n" +
            "      <format>text/x-wiki</format>\n" +
            "      <text xml:space=\"preserve\" bytes=\"6523\">[[M]]</text>\n" +
            "    </revision>\n" +
            "  </page>";

    @Before
    public void setUp() {
        WikiPageLinksMapper mapper = new WikiPageLinksMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(testPageA));
        mapDriver.withInput(new LongWritable(2), new Text(testPageM));
        mapDriver.withInput(new LongWritable(3), new Text(testPageY));
        mapDriver.withOutput(new Text("A"), new Text("Y"));
        mapDriver.withOutput(new Text("A"), new Text("M"));
        mapDriver.withOutput(new Text("Y"), new Text("A"));
        mapDriver.withOutput(new Text("Y"), new Text("Y"));
        mapDriver.withOutput(new Text("M"), new Text("M"));
        mapDriver.runTest(false);
    }
}
