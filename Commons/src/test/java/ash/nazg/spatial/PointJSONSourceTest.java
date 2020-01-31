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
import static org.junit.Assert.assertNull;

public class PointJSONSourceTest {
    @Test
    public void sourceTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.json.points.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Point> rddS = (JavaRDD<Point>) ret.get("poi");
            assertEquals(
                    15,
                    rddS.count()
            );

            List<MapWritable> datas = rddS
                    .map(t -> (MapWritable) t.getUserData())
                    .collect();

            Text name = new Text("_radius");
            for (MapWritable data : datas) {
                assertEquals(5, data.size());
                assertEquals(300.D, ((DoubleWritable)data.get(name)).get(), 0.D);
            }
        }
    }

    @Test
    public void customTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.json.points.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Point> rddS = (JavaRDD<Point>) ret.get("custom");
            assertEquals(
                    15,
                    rddS.count()
            );

            List<MapWritable> datas = rddS
                    .map(t -> (MapWritable) t.getUserData())
                    .collect();

            Text value = new Text("value");
            Text custom = new Text("custom");
            Text name = new Text("name");

            for (MapWritable data : datas) {
                assertEquals(3, data.size());
                assertEquals(value, data.get(custom));
                assertNull(data.get(name));
            }
        }
    }
}
