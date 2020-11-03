package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import ash.nazg.spatial.TrackSegment;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringWriter;
import java.util.*;

import static ash.nazg.spatial.config.ConfigurationParameters.RDD_OUTPUT_POINTS;
import static ash.nazg.spatial.config.ConfigurationParameters.RDD_OUTPUT_TRACKS;

@SuppressWarnings("unused")
public class TrackCSVOutputOperation extends Operation {
    private static final String VERB = "trackCsvOutput";

    @Description("What to output to 'tracks' output")
    public static final String OP_OUTPUT_MODE = "tracks.mode";
    @Description("By default, output both Tracks' and Track segments' data")
    public static final OutputMode DEF_OUTPUT_MODE = OutputMode.BOTH;

    private String inputName;
    private String outputTracks;
    private String outputPoints;
    private char outputDelimiterTracks;
    private char outputDelimiterPoints;
    private List<String> outputColumnsTracks;
    private List<String> outputColumnsPoints;
    private OutputMode outputMode;

    @Override
    @Description("Take a Track RDD and produce a CSV file")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_OUTPUT_MODE, OutputMode.class, DEF_OUTPUT_MODE)
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_TRACKS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_POINTS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                )
                        }
                )
        );
    }

    @Override
    public void configure(Properties config, Properties variables) throws InvalidConfigValueException {
        super.configure(config, variables);

        inputName = describedProps.inputs.get(0);
        outputTracks = describedProps.namedOutputs.get(RDD_OUTPUT_TRACKS);
        outputPoints = describedProps.namedOutputs.get(RDD_OUTPUT_POINTS);

        outputDelimiterTracks = dataStreamsProps.outputDelimiter(outputTracks);
        outputDelimiterPoints = dataStreamsProps.outputDelimiter(outputPoints);
        outputColumnsTracks = Arrays.asList(dataStreamsProps.outputColumns.get(outputTracks));
        outputColumnsPoints = Arrays.asList(dataStreamsProps.outputColumns.get(outputPoints));

        outputMode = describedProps.defs.getTyped(OP_OUTPUT_MODE);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<SegmentedTrack> tracks = (JavaRDD<SegmentedTrack>) input.get(inputName);
        Map<String, JavaRDDLike> retMap = new HashMap<>();

        if (outputTracks != null) {
            final char _outputDelimiterStats = outputDelimiterTracks;
            final List<String> _outputColumnsStats = outputColumnsTracks;
            final OutputMode _outputMode = outputMode;

            JavaRDD<Text> output = tracks
                    .mapPartitions(it -> {
                        List<Text> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            SegmentedTrack next = it.next();

                            List<MapWritable> props = new ArrayList<>();
                            if (_outputMode != OutputMode.SEGMENTS) {
                                props.add((MapWritable) next.getUserData());
                            }
                            if (_outputMode != OutputMode.TRACKS) {
                                for (Geometry g : next.geometries()) {
                                    props.add((MapWritable) g.getUserData());
                                }
                            }

                            for (MapWritable t : props) {
                                String[] out = new String[_outputColumnsStats.size()];

                                int i = 0;
                                for (String column : _outputColumnsStats) {
                                    out[i++] = t.get(new Text(column)).toString();
                                }

                                StringWriter buffer = new StringWriter();
                                CSVWriter writer = new CSVWriter(buffer, _outputDelimiterStats, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                                writer.writeNext(out, false);
                                writer.close();

                                ret.add(new Text(buffer.toString()));
                            }
                        }

                        return ret.iterator();
                    });

            retMap.put(outputTracks, output);
        }

        if (outputPoints != null) {
            final char _outputDelimiterPoints = outputDelimiterPoints;
            final List<String> _outputColumnsPoints = outputColumnsPoints;

            JavaRDD<Text> output = tracks
                    .mapPartitions(it -> {
                        List<Text> ret = new ArrayList<>();

                        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
                        final DocumentBuilder b = f.newDocumentBuilder();

                        while (it.hasNext()) {
                            SegmentedTrack next = it.next();

                            for (Geometry g : next.geometries()) {
                                TrackSegment s = (TrackSegment) g;
                                for (Geometry gg : s.geometries()) {
                                    Point p = (Point) gg;
                                    MapWritable t = (MapWritable) p.getUserData();

                                    String[] out = new String[_outputColumnsPoints.size()];

                                    int i = 0;
                                    for (String column : _outputColumnsPoints) {
                                        out[i++] = t.get(new Text(column)).toString();
                                    }

                                    StringWriter buffer = new StringWriter();
                                    CSVWriter writer = new CSVWriter(buffer, _outputDelimiterPoints, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                            CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                                    writer.writeNext(out, false);
                                    writer.close();

                                    ret.add(new Text(buffer.toString()));
                                }
                            }
                        }

                        return ret.iterator();
                    });

            retMap.put(outputPoints, output);
        }

        return retMap;
    }

    public enum OutputMode {
        @Description("Output only Track segments' data")
        SEGMENTS,
        @Description("Output only Tracks' data")
        TRACKS,
        @Description("Output both Tracks' and then each Track segments' data")
        BOTH
    }
}
