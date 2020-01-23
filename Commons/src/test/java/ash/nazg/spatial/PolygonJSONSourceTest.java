package ash.nazg.spatial;

import ash.nazg.spark.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PolygonJSONSourceTest {
    @Test
    public void sourceTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.json.polygons.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Polygon> rddS = (JavaRDD<Polygon>) ret.get("poi");
            assertEquals(
                    1,
                    rddS.count()
            );

        }
    }
}
