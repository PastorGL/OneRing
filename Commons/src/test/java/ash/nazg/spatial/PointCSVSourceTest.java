/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.locationtech.jts.geom.Point;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PointCSVSourceTest {
    @Test
    public void sourceTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.csv.points.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Point> rddS = (JavaRDD<Point>) ret.get("poi");
            assertEquals(
                    12,
                    rddS.count()
            );

            List<Double> radii = rddS
                    .map(t -> ((DoubleWritable) ((MapWritable) t.getUserData()).get(new Text("_radius"))).get())
                    .collect();

            for (Double radius : radii) {
                assertNotEquals(300.D, radius, 0.D);
            }

        }
    }
}
