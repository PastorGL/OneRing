/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import ash.nazg.spatial.TrackSegment;
import org.apache.hadoop.io.MapWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class TrackPointOutputOperation extends Operation {
    private String inputName;

    private String outputName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("trackPointOutput", "Take a Track RDD and produce a Point RDD while retaining all properties",

                new PositionalStreamsMetaBuilder()
                        .ds("SegmentedTrack RDD",
                                new StreamType[]{StreamType.Track}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("Point RDD",
                                new StreamType[]{StreamType.Point}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Point> output = ((JavaRDD<SegmentedTrack>) input.get(inputName))
                .mapPartitions(it -> {
                    List<Point> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        SegmentedTrack next = it.next();
                        MapWritable tt = (MapWritable) next.getUserData();

                        for (Geometry g : next.geometries()) {
                            TrackSegment s = (TrackSegment) g;

                            MapWritable st = new MapWritable(tt);
                            st.putAll((MapWritable) s.getUserData());

                            for (Geometry gg : s.geometries()) {
                                Point p = (Point) gg;

                                MapWritable pt = new MapWritable(st);
                                pt.putAll((MapWritable) p.getUserData());

                                p.setUserData(pt);
                                ret.add(p);
                            }
                        }
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
