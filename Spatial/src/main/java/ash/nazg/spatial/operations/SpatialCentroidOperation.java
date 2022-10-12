/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.data.DataStream;
import ash.nazg.data.StreamType;
import ash.nazg.data.spatial.PointEx;
import ash.nazg.data.spatial.PolygonEx;
import ash.nazg.data.spatial.SegmentedTrack;
import ash.nazg.data.spatial.TrackSegment;
import ash.nazg.metadata.*;
import ash.nazg.scripting.Operation;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import java.util.*;

@SuppressWarnings("unused")
public class SpatialCentroidOperation extends Operation {
    public static final String TRACKS_MODE = "tracks.mode";

    private TracksMode tracksMode;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("spatialCentroid", "Take a Track or Polygon DataStream and extract a Point DataStream" +
                " of centroids while keeping all other properties",

                new PositionalStreamsMetaBuilder()
                        .input("SegmentedTrack or Polygon RDD",
                                new StreamType[]{StreamType.Polygon, StreamType.Track}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(TRACKS_MODE, "What to output for SegmentedTrack RDDs", TracksMode.class,
                                TracksMode.BOTH, "By default, output both SegmentedTrack's and TrackSegment's data")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Point DataStream with centroids. Each has radius set",
                                new StreamType[]{StreamType.Point}, Origin.GENERATED, null
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        tracksMode = params.get(TRACKS_MODE);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final TracksMode _tracksMode = tracksMode;

        DataStream input = inputStreams.getValue(0);

        JavaRDD<PointEx> output = ((JavaRDD<Object>) input.get())
                .mapPartitions(it -> {
                    List<PointEx> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Geometry g = (Geometry) it.next();

                        if (g instanceof PolygonEx) {
                            PointEx centroid = ((PolygonEx) g).centrePoint;

                            HashMap<String, Object> props = new HashMap<>((HashMap<String, Object>) g.getUserData());

                            centroid.setUserData(props);
                            ret.add(centroid);
                        } else {
                            HashMap<String, Object> trackProps = (HashMap<String, Object>) g.getUserData();

                            if (_tracksMode != TracksMode.SEGMENTS) {
                                PointEx centroid = ((SegmentedTrack) g).centrePoint;

                                HashMap<String, Object> props = new HashMap<>(trackProps);

                                centroid.setUserData(props);
                                ret.add(centroid);
                            }

                            if (_tracksMode != TracksMode.TRACKS) {
                                for (Geometry gg : ((SegmentedTrack) g).geometries()) {
                                    PointEx centroid = ((TrackSegment) gg).centrePoint;

                                    HashMap<String, Object> props = new HashMap<>(trackProps);
                                    props.putAll((HashMap<String, Object>) gg.getUserData());

                                    centroid.setUserData(props);
                                    ret.add(centroid);
                                }
                            }
                        }
                    }

                    return ret.iterator();
                });

        List<String> outputColumns = null;
        switch (input.streamType) {
            case Point: {
                outputColumns = input.accessor.attributes("point");
                break;
            }
            case Track: {
                switch (tracksMode) {
                    case SEGMENTS: {
                        outputColumns = input.accessor.attributes("segment");
                        break;
                    }
                    case TRACKS: {
                        outputColumns = input.accessor.attributes("track");
                        break;
                    }
                    default: {
                        outputColumns = new ArrayList<>(input.accessor.attributes("track"));
                        outputColumns.addAll(input.accessor.attributes("segment"));
                    }
                }
                break;
            }
            case Polygon: {
                outputColumns = input.accessor.attributes("polygon");
                break;
            }
        }

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Point, output, Collections.singletonMap("point", outputColumns)));
    }

    public enum TracksMode implements DefinitionEnum {
        SEGMENTS("Output only TrackSegments' centroids"),
        TRACKS("Output only SegmentedTracks' centroids"),
        BOTH("Output both SegmentedTracks' and then each of their TrackSegments' centroids");

        private final String descr;

        TracksMode(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
