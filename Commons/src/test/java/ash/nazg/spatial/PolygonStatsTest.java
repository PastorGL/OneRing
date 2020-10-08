package ash.nazg.spatial;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PolygonStatsTest {
    @Test
    public void polygonStatsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.polygon.stats.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Polygon> rddS = (JavaRDD<Polygon>) ret.get("stats");
            assertEquals(14, rddS.count());

            List<MapWritable> datas = rddS
                    .map(t -> (MapWritable) t.getUserData())
                    .collect();

            Text perimeter = new Text("_perimeter");
            Text area = new Text("_area");
            for (MapWritable data : datas) {
                assertTrue(((DoubleWritable) data.get(perimeter)).get() > 0.D);
                assertTrue(((DoubleWritable) data.get(area)).get() > 0.D);
            }
        }
    }
}
