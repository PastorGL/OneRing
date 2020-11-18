package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import ash.nazg.spatial.TrackSegment;
import org.apache.hadoop.io.MapWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.*;

@SuppressWarnings("unused")
public class TrackPointOutputOperation extends Operation {
    private static final String VERB = "trackPointOutput";

    private String inputName;
    private String outputName;

    @Override
    @Description("Take a Track RDD and produce a Point RDD while retaining all properties")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                false
                        )
                )
        );
    }

    @Override
    public void configure(Properties config, Properties variables) throws InvalidConfigValueException {
        super.configure(config, variables);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);
    }

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
