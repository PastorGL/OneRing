/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity;

import ash.nazg.spark.TestRunner;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.locationtech.jts.geom.Point;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ProximityFilterTest {

    @Test
    public void proximityFilterTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/config.proximity.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("output");

            assertEquals(44, resultRDD.count());

            List<Text> sample = resultRDD.collect();
            for (Text s : sample) {
                double dist = new Double(s.toString().split(",", 5)[4]);
                assertTrue(dist <= 30000.D);
            }

            JavaRDD<Point> evicted = (JavaRDD<Point>) ret.get("evicted");

            assertTrue(2761 - 44 >= evicted.count());

            List<Point> evs = evicted.sample(false, 0.01).collect();
            List<Point> prox = ret.get("geometries").collect();
            for (Point e : evs) {
                for (Point x : prox) {
                    double dist = Geodesic.WGS84.Inverse(e.getY(), e.getX(), x.getY(), x.getX(), GeodesicMask.DISTANCE).s12;
                    assertTrue(dist > 30000.D);
                }
            }
        }
    }
}
