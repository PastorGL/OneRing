package ash.nazg.spatial;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.locationtech.jts.geom.Point;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TrackSourcesTest {
    @Test
    public void sourceOutputGpxTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.track.gpx.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Point> rddS = (JavaRDD<Point>) ret.get("output");
            assertEquals(
                    12,
                    rddS.count()
            );
        }
    }

    @Test
    public void sourceOutputCsvTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.track.csv.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("stats");
            assertEquals(
                    14,
                    rddS.count()
            );

            List<MapWritable> pts = ((JavaRDD<Point>) ret.get("points"))
                    .map(e->(MapWritable)e.getUserData()).collect();
            assertEquals(
                    37,
                    pts.size()
            );
        }
    }
}
