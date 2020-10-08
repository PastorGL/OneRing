package ash.nazg.spatial;

import ash.nazg.spark.TestRunner;
import io.jenetics.jpx.Track;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.w3c.dom.Element;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TrackStatsTest {
    @Test
    public void trackStatsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.track.stats.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Track> rddS = (JavaRDD<Track>) ret.get("stats");
            assertEquals(
                    12,
                    rddS.count()
            );

            List<Element> datas = rddS
                    .map(t -> t.getExtensions().get().getDocumentElement())
                    .collect();

            for (Element data : datas) {
                assertTrue(Double.parseDouble(data.getElementsByTagName("_duration").item(0).getTextContent()) > 0.D);
                assertTrue(Double.parseDouble(data.getElementsByTagName("_range").item(0).getTextContent()) > 0.D);
                assertTrue(Double.parseDouble(data.getElementsByTagName("_distance").item(0).getTextContent()) > 0.D);
                assertTrue(Double.parseDouble(data.getElementsByTagName("_points").item(0).getTextContent()) > 0.D);
            }

            Element data = datas.get(11);
            assertEquals(14 * 60 + 22, Double.parseDouble(data.getElementsByTagName("_duration").item(0).getTextContent()), 2);
            assertEquals(2_488.D, Double.parseDouble(data.getElementsByTagName("_distance").item(0).getTextContent()), 2);
            assertEquals(141, Integer.parseInt(data.getElementsByTagName("_points").item(0).getTextContent()));

            data = datas.get(10);
            assertEquals(2_864, Double.parseDouble(data.getElementsByTagName("_duration").item(0).getTextContent()), 2);
            assertEquals(2_446 * 1.6, Double.parseDouble(data.getElementsByTagName("_distance").item(0).getTextContent()), 20);
            assertEquals(287, Integer.parseInt(data.getElementsByTagName("_points").item(0).getTextContent()));
        }
    }
}
