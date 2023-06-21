/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.data.spatial.PointEx;
import ash.nazg.data.spatial.SegmentedTrack;
import ash.nazg.data.spatial.TrackSegment;
import io.jenetics.jpx.GPX;
import io.jenetics.jpx.Track;
import io.jenetics.jpx.WayPoint;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class TrackToGpxTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("trackToGpx", StreamType.Track, StreamType.PlainText,
                "Take a SegmentedTrack DataStream and produce a GPX fragment file",

                new DefinitionMetaBuilder()
                        .def("name.attr", "Attribute of Segmented Track that becomes GPX track name")
                        .def("ts.attr", "Attribute of Points that becomes GPX time stamp", null, "By default, don't set time stamp")
                        .build(),
                null

        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            String nameColumn = params.get("name.attr");
            String timeColumn = params.get("ts.attr");

            return new DataStream(StreamType.PlainText,
                    ((JavaRDD<SegmentedTrack>) ds.get()).mapPartitions(it -> {
                        List<Text> result = new ArrayList<>();

                        GPX.Writer writer = GPX.writer();

                        while (it.hasNext()) {
                            SegmentedTrack trk = it.next();

                            GPX.Builder gpx = GPX.builder();
                            gpx.creator("OneRing");
                            Track.Builder trkBuilder = Track.builder();

                            for (Geometry g : trk) {
                                TrackSegment ts = (TrackSegment) g;
                                io.jenetics.jpx.TrackSegment.Builder segBuilder = io.jenetics.jpx.TrackSegment.builder();

                                for (Geometry gg : ts) {
                                    PointEx p = (PointEx) gg;
                                    WayPoint.Builder wp = WayPoint.builder();
                                    wp.lat(p.getY());
                                    wp.lon(p.getX());
                                    if (timeColumn != null) {
                                        wp.time(((Long) ((Map<String, Object>) p.getUserData()).get(timeColumn)));
                                    }

                                    segBuilder.addPoint(wp.build());
                                }

                                trkBuilder.addSegment(segBuilder.build());
                            }

                            if (nameColumn != null) {
                                trkBuilder.name(String.valueOf(((Map<String, Object>) trk.getUserData()).get(nameColumn)));
                            }
                            gpx.addTrack(trkBuilder.build());

                            result.add(new Text(writer.toString(gpx.build())));
                        }

                        return result.iterator();
                    }), newColumns);
        };
    }
}
