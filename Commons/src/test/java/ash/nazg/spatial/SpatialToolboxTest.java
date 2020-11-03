package ash.nazg.spatial;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpatialToolboxTest {
    @Test
    public void spatialToolboxTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.spatialToolbox.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<SegmentedTrack> rddS = (JavaRDD<SegmentedTrack>) ret.get("ret1");

            SegmentedTrack st = rddS.first();
            assertEquals(2, st.getNumGeometries());
            assertEquals(
                    10,
                    st.getGeometryN(0).getNumGeometries()
            );

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret2");

            st = rddS.first();
            List<Geometry> points = new ArrayList<>();
            for (int i = st.getNumGeometries() - 1; i>=0 ; i--){
                points.addAll(Arrays.asList(((TrackSegment)st.getGeometryN(i)).geometries()));
            }
            List<MapWritable> datas = points.stream()
                    .map(t -> (MapWritable) t.getUserData())
                    .collect(Collectors.toList());
            assertEquals(4, datas.size());
            for (MapWritable data : datas) {
                double acc = Double.parseDouble(data.get(new Text("acc")).toString());
                assertTrue(acc >= 15.D);
                assertTrue(acc < 100.D);
            }

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret3");

            st = rddS.first();
            points = new ArrayList<>();
            for (int i = st.getNumGeometries() - 1; i>=0 ; i--){
                points.addAll(Arrays.asList(((TrackSegment)st.getGeometryN(i)).geometries()));
            }
            datas = points.stream()
                    .map(t -> (MapWritable) t.getUserData())
                    .collect(Collectors.toList());
            assertEquals(15, datas.size());
            Pattern p = Pattern.compile(".+?non.*");
            for (MapWritable data : datas) {
                String pt = data.get(new Text("pt")).toString();
                String trackid = data.get(new Text("trackid")).toString();
                assertTrue("e2e".equals(pt) || p.matcher(trackid).matches());
            }

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret4");

            st = rddS.first();
            assertEquals(13, st.getNumGeometries());

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret5");

            st = rddS.first();
            datas = Arrays.stream(st.geometries())
                    .map(t -> (MapWritable) t.getUserData())
                    .collect(Collectors.toList());
            assertEquals(2, datas.size());
            for (MapWritable data : datas) {
                assertTrue(Integer.parseInt(data.get(new Text("_points")).toString()) > 3);
            }
        }
    }
}
