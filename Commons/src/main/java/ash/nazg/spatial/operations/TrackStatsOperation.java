package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.functions.GPXExtensions;
import io.jenetics.jpx.Track;
import io.jenetics.jpx.TrackSegment;
import io.jenetics.jpx.WayPoint;
import net.sf.geographiclib.Geodesic;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.*;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class TrackStatsOperation extends Operation {
    public static final String VERB = "trackStats";
    @Description("Pin distance calculation for entire track to its first point")
    public static final String OP_DISTANCE_PIN = "distance.pin";
    @Description("By default, do not pin")
    public static final Boolean DEF_DISTANCE_PIN = false;

    private String inputName;
    private String outputName;

    private Boolean pinDistance;

    @Override
    @Description("Take a Track RDD and augment its properties with statistics")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_DISTANCE_PIN, Boolean.class, DEF_DISTANCE_PIN)
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
                                new String[]{GEN_USERID, GEN_POINTS, GEN_TRACKID, GEN_DURATION, GEN_RANGE, GEN_DISTANCE}
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);

        pinDistance = describedProps.defs.getTyped(OP_DISTANCE_PIN);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final boolean _pinDistance = pinDistance;

        JavaRDD<Track> output = ((JavaRDD<Track>) input.get(inputName))
                .mapPartitions(it -> {
                    Text distanceAttr = new Text(GEN_DISTANCE);
                    Text pointsAttr = new Text(GEN_POINTS);

                    List<Track> result = new ArrayList<>();

                    DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
                    final DocumentBuilder b = f.newDocumentBuilder();

                    while (it.hasNext()) {
                        Track next = it.next();
                        Track.Builder aug = Track.builder();

                        WayPoint start = null;

                        int augPoints = 0;
                        double augDistance = 0.D;
                        double augRange = 0.D;
                        long augDuration = 0L;
                        for (TrackSegment seg : next) {
                            TrackSegment.Builder augSeg = TrackSegment.builder();

                            List<WayPoint> wayPoints = seg.getPoints();
                            int segPoints = wayPoints.size();
                            double segDistance = 0.D;
                            double segRange = 0.D;
                            long segDuration = 0L;

                            WayPoint prev;
                            int s = 1;
                            int d = 0;
                            if (_pinDistance) {
                                if (start == null) {
                                    start = wayPoints.get(0);
                                    prev = wayPoints.get(1);
                                    s = 2;
                                    d = -1;

                                    augSeg.points(wayPoints.subList(1, wayPoints.size()));
                                } else {
                                    prev = wayPoints.get(0);

                                    augSeg.points(wayPoints);
                                }
                            } else {
                                start = wayPoints.get(0);
                                prev = start;

                                augSeg.points(wayPoints);
                            }
                            for (int i = s; i < segPoints; i++) {
                                WayPoint point = wayPoints.get(i);

                                segDuration += point.getTime().get().toEpochSecond() - prev.getTime().get().toEpochSecond();
                                segDistance += Geodesic.WGS84.Inverse(prev.getLatitude().doubleValue(), prev.getLongitude().doubleValue(),
                                        point.getLatitude().doubleValue(), point.getLongitude().doubleValue()).s12;
                                segRange = Math.max(segRange, Geodesic.WGS84.Inverse(start.getLatitude().doubleValue(), start.getLongitude().doubleValue(),
                                        point.getLatitude().doubleValue(), point.getLongitude().doubleValue()).s12);
                                prev = point;
                            }

                            augDuration += segDuration;
                            augDistance += segDistance;
                            augRange = Math.max(augRange, segRange);
                            augPoints += segPoints + d;

                            Document segProps = seg.getExtensions().orElseGet(b::newDocument);
                            Element segPropsEl = GPXExtensions.getOrCreate(segProps);
                            GPXExtensions.appendExt(segPropsEl, GEN_DURATION, segDuration);
                            GPXExtensions.appendExt(segPropsEl, GEN_DISTANCE, segDistance);
                            GPXExtensions.appendExt(segPropsEl, GEN_POINTS, segPoints + d);
                            GPXExtensions.appendExt(segPropsEl, GEN_RANGE, segRange);
                            augSeg.extensions(segProps);

                            aug.addSegment(augSeg.build());
                        }

                        Document augProps = next.getExtensions().orElseGet(b::newDocument);
                        Element augPropsEl = GPXExtensions.getOrCreate(augProps);
                        GPXExtensions.appendExt(augPropsEl, GEN_DURATION, augDuration);
                        GPXExtensions.appendExt(augPropsEl, GEN_DISTANCE, augDistance);
                        GPXExtensions.appendExt(augPropsEl, GEN_POINTS, augPoints);
                        GPXExtensions.appendExt(augPropsEl, GEN_RANGE, augRange);
                        aug.extensions(augProps);

                        result.add(aug.build());
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }

}
