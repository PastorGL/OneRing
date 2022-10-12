/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.data.DataStream;
import ash.nazg.data.StreamConverter;
import ash.nazg.data.StreamType;
import ash.nazg.data.Transform;
import ash.nazg.data.spatial.PointEx;
import ash.nazg.data.spatial.SegmentedTrack;
import ash.nazg.data.spatial.TrackSegment;
import ash.nazg.metadata.TransformMeta;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import java.util.*;

@SuppressWarnings("unused")
public class TrackToPointTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("trackToPoint", StreamType.Track, StreamType.Point,
                "Extracts all Points from SegmentedTrack DataStream",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final List<String> _outputColumns = newColumns.get("point");

            List<String> outColumns = new ArrayList<>();
            if (_outputColumns != null) {
                outColumns.addAll(_outputColumns);
            } else {
                outColumns.addAll(ds.accessor.attributes("track"));
                outColumns.addAll(ds.accessor.attributes("segment"));
                outColumns.addAll(ds.accessor.attributes("point"));
            }

            return new DataStream(StreamType.Point,
                    ((JavaRDD<SegmentedTrack>) ds.get()).mapPartitions(it -> {
                        List<PointEx> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            SegmentedTrack next = it.next();
                            Map<String, Object> trkData = (Map) next.getUserData();

                            for (Geometry g : next.geometries()) {
                                TrackSegment s = (TrackSegment) g;
                                Map<String, Object> segData = (Map) g.getUserData();

                                for (Geometry gg : s.geometries()) {
                                    Map<String, Object> pntProps = new HashMap<>(trkData);
                                    pntProps.putAll(segData);
                                    pntProps.putAll((Map) gg.getUserData());

                                    PointEx p = new PointEx(gg);
                                    if (_outputColumns != null) {
                                        for (String prop : _outputColumns) {
                                            p.put(prop, pntProps.get(prop));
                                        }
                                    } else {
                                        p.put(pntProps);
                                    }

                                    ret.add(p);
                                }
                            }
                        }

                        return ret.iterator();
                    }), Collections.singletonMap("point", outColumns));
        };
    }
}
