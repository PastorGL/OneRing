/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial;

import ash.nazg.data.spatial.SegmentedTrack;
import ash.nazg.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TrackStatsTest {
    @Test
    public void trackStatsTest() {
        try (TestRunner underTest = new TestRunner("/test.trackStats.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<SegmentedTrack> tracks = ((JavaRDD<SegmentedTrack>) ret.get("stats")).collect();
            Assert.assertEquals(
                    12,
                    tracks.size()
            );

            for (SegmentedTrack data : tracks) {
                Assert.assertTrue(data.asDouble("_duration") > 0.D);
                Assert.assertTrue(data.asDouble("_radius") > 0.D);
                Assert.assertTrue(data.asDouble("_distance") > 0.D);
                Assert.assertTrue(data.asDouble("_points") > 0.D);
            }

            SegmentedTrack data = tracks.get(11);
            Assert.assertEquals(14 * 60 + 22, data.asDouble("_duration"), 2);
            Assert.assertEquals(2_488.D, data.asDouble("_distance"), 2);
            Assert.assertEquals(141, data.asInt("_points").intValue());

            data = tracks.get(10);
            Assert.assertEquals(2_864, data.asDouble("_duration"), 2);
            Assert.assertEquals(2_446 * 1.6, data.asDouble("_distance"), 20);
            Assert.assertEquals(287, data.asInt("_points").intValue());
        }
    }
}
