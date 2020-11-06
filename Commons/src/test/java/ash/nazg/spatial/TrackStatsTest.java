package ash.nazg.spatial;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TrackStatsTest {
    @Test
    public void trackStatsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.track.stats.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<SegmentedTrack> rddS = (JavaRDD<SegmentedTrack>) ret.get("stats");
            assertEquals(
                    12,
                    rddS.count()
            );

            List<MapWritable> datas = rddS
                    .map(t -> (MapWritable) t.getUserData())
                    .collect();

            for (MapWritable data : datas) {
                assertTrue(Double.parseDouble(data.get(new Text("_duration")).toString()) > 0.D);
                assertTrue(Double.parseDouble(data.get(new Text("_radius")).toString()) > 0.D);
                assertTrue(Double.parseDouble(data.get(new Text("_distance")).toString()) > 0.D);
                assertTrue(Double.parseDouble(data.get(new Text("_points")).toString()) > 0.D);
            }

            MapWritable data = datas.get(11);
            assertEquals(14 * 60 + 22, Double.parseDouble(data.get(new Text("_duration")).toString()), 2);
            assertEquals(2_488.D, Double.parseDouble(data.get(new Text("_distance")).toString()), 2);
            assertEquals(141, Integer.parseInt(data.get(new Text("_points")).toString()));

            data = datas.get(10);
            assertEquals(2_864, Double.parseDouble(data.get(new Text("_duration")).toString()), 2);
            assertEquals(2_446 * 1.6, Double.parseDouble(data.get(new Text("_distance")).toString()), 20);
            assertEquals(287, Integer.parseInt(data.get(new Text("_points")).toString()));
        }
    }
}
