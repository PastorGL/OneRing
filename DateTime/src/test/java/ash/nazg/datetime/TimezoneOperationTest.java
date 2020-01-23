package ash.nazg.datetime;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;

public class TimezoneOperationTest {

    @Test
    public void timezoneOperationTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test2.timezone.properties")) {

            JavaRDD<Text> dataset = (JavaRDD<Text>) underTest.go().get("signals_output");

            Assert.assertEquals(5000, dataset.count());

            List<Text> sample = dataset.takeOrdered(1);

            Assert.assertEquals(
                    "0,51.09022,1.543081,59a3e4ffd1a19,1469583507,2016-07-27T01:38:27+04:00[Europe/Samara],3,27,7,2016,1,38,2016-07-26T21:38:27Z[GMT],2,26,7,2016,21,38",
                    sample.get(0).toString()
            );

        }
    }

    @Test
    public void customTimestampFormatTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.timezone.properties")) {

            JavaRDD source = (JavaRDD) underTest.go().get("signals");
            JavaRDD dataset = (JavaRDD) underTest.go().get("signals_output");

            Assert.assertEquals(10, dataset.count());

            List<String> srcCol = source.map(t -> t.toString()).collect();
            List<String> collected = dataset.map(t -> t.toString()).collect();

            Map<Integer, String> srcParsed = srcCol.stream()
                    .map(l -> l.split(","))
                    .collect(Collectors.toMap(
                            l -> new Integer(l[1]),
                            l -> l[0]
                    ));
            Map<Integer, String> collParsed = collected.stream()
                    .map(l -> l.split(","))
                    .collect(Collectors.toMap(
                            l -> new Integer(l[1]),
                            l -> l[0]
                    ));

            for (Map.Entry<Integer, String> s : srcParsed.entrySet()) {
                assertFalse(collParsed.get(s.getKey()).equalsIgnoreCase(s.getValue()));
            }

        }
    }
}
