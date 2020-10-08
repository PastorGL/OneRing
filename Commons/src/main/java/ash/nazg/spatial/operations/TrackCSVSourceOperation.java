package ash.nazg.spatial.operations;

import ash.nazg.commons.functions.KeyCountPerPartitionFunction;
import ash.nazg.commons.functions.TrackComparator;
import ash.nazg.commons.functions.TrackPartitioner;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.config.ConfigurationParameters;
import ash.nazg.spatial.functions.GPXExtensions;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import io.jenetics.jpx.Track;
import io.jenetics.jpx.TrackSegment;
import io.jenetics.jpx.WayPoint;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.broadcast.Broadcast;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import scala.Tuple2;
import scala.Tuple4;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class TrackCSVSourceOperation extends Operation {
    @Description("By default, create single-segmented tracks")
    public static final String DEF_CSV_TRACKID_COLUMN = null;

    public static final String VERB = "trackCsvSource";

    private String inputName;
    private char inputDelimiter;
    private Map<String, Integer> inputColumns;
    private int latColumn;
    private int lonColumn;
    private int useridColumn;
    private int tsColumn;
    private Integer trackColumn;

    private String outputName;
    private Map<String, Integer> outputColumns;

    @Override
    @Description("Source user Tracks from CSV file with signal data")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_CSV_TIMESTAMP_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_CSV_LAT_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_CSV_LON_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_CSV_USERID_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_CSV_TRACKID_COLUMN, DEF_CSV_TRACKID_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
                                true
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);

        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);

        inputColumns = dataStreamsProps.inputColumns.get(inputName);
        List<String> outColumns = Arrays.asList(dataStreamsProps.outputColumns.get(outputName));
        outputColumns = inputColumns.entrySet().stream()
                .filter(c -> (outColumns.size() == 0) || outColumns.contains(c.getKey()))
                .collect(Collectors.toMap(c -> c.getKey().replaceFirst("^[^.]+\\.", ""), Map.Entry::getValue));;

        String prop;

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CSV_USERID_COLUMN);
        useridColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CSV_TRACKID_COLUMN);
        trackColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CSV_LAT_COLUMN);
        latColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CSV_LON_COLUMN);
        lonColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CSV_TIMESTAMP_COLUMN);
        tsColumn = inputColumns.get(prop);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final int _latColumn = latColumn;
        final int _lonColumn = lonColumn;
        final int _useridColumn = useridColumn;
        final int _tsColumn = tsColumn;
        final Integer _trackColumn = trackColumn;
        final Map<String, Integer> _outputColumns = outputColumns;

        JavaRDD<Object> signalsInput = (JavaRDD<Object>) input.get(inputName);
        int _numPartitions = signalsInput.getNumPartitions();

        JavaPairRDD<Tuple2<Text, Double>, Tuple4<Double, Double, Text, Text>> signals = signalsInput
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Tuple2<Text, Double>, Tuple4<Double, Double, Text, Text>>> ret = new ArrayList<>();
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter)
                            .build();

                    while (it.hasNext()) {
                        Object o = it.next();
                        String l = o instanceof String ? (String) o : String.valueOf(o);
                        String[] row = parser.parseLine(l);

                        Text userId = new Text(row[_useridColumn]);
                        Double lat = new Double(row[_latColumn]);
                        Double lon = new Double(row[_lonColumn]);
                        Double timestamp = new Double(row[_tsColumn]);

                        Text track = (_trackColumn != null) ? new Text(row[_trackColumn]) : null;

                        ret.add(new Tuple2<>(new Tuple2<>(userId, timestamp), new Tuple4<>(lat, lon, track, new Text(l))));
                    }

                    return ret.iterator();
                })
                .repartitionAndSortWithinPartitions(new TrackPartitioner(_numPartitions), new TrackComparator()) // pre-sort by timestamp
                ;

        Broadcast<HashMap<Integer, Integer>> num = ctx.broadcast(new KeyCountPerPartitionFunction<Tuple4<Double, Double, Text, Text>>()
                .call(signals));

        JavaRDD<Track> output = signals.mapPartitionsWithIndex((idx, it) -> {
            int useridCount = num.getValue().get(idx);
            boolean isSegmented = (_trackColumn != null);

            Object[] result = new Object[useridCount];

            Map<Text, Integer> useridOrd = new HashMap<>();

            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            final DocumentBuilder b = f.newDocumentBuilder();

            CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

            Text[] userids = new Text[useridCount];
            int n = 0;
            while (it.hasNext()) {
                Tuple2<Tuple2<Text, Double>, Tuple4<Double, Double, Text, Text>> line = it.next();

                Text userid = line._1._1;
                int current;
                if (useridOrd.containsKey(userid)) {
                    current = useridOrd.get(userid);
                } else {
                    useridOrd.put(userid, n);
                    userids[n] = userid;
                    current = n;

                    n++;
                }

                List<TrackSegment.Builder> segments = (List<TrackSegment.Builder>) result[current];
                if (segments == null) {
                    segments = new ArrayList<>();
                    result[current] = segments;
                }

                TrackSegment.Builder segment;
                String trackId = null;
                if (isSegmented) {
                    trackId = line._2._3().toString();

                    String lastTrackId = null;
                    TrackSegment.Builder lastSegment = null;
                    if (segments.size() != 0) {
                        lastSegment = segments.get(segments.size() - 1);
                        List<WayPoint> points = lastSegment.points();
                        lastTrackId = points.get(points.size() - 1).getComment().get();
                    }

                    if (trackId.equals(lastTrackId)) {
                        segment = lastSegment;
                    } else {
                        segment = TrackSegment.builder();

                        Document segProps = b.newDocument();
                        Element segPropsEl = GPXExtensions.getOrCreate(segProps);
                        GPXExtensions.appendExt(segPropsEl, GEN_USERID, userid.toString());
                        GPXExtensions.appendExt(segPropsEl, GEN_TRACKID, trackId);
                        segment.extensions(segProps);

                        segments.add(segment);
                    }
                } else {
                    if (segments.size() == 0) {
                        segment = TrackSegment.builder();

                        Document segProps = b.newDocument();
                        Element segPropsEl = GPXExtensions.getOrCreate(segProps);
                        GPXExtensions.appendExt(segPropsEl, GEN_USERID, userid.toString());
                        segment.extensions(segProps);

                        segments.add(segment);
                    } else {
                        segment = segments.get(0);
                    }
                }

                Document pointProps = b.newDocument();
                Element pointPropsEl = GPXExtensions.getOrCreate(pointProps);
                String[] row = parser.parseLine(line._2._4().toString());
                for (Map.Entry<String, Integer> col : _outputColumns.entrySet()) {
                    GPXExtensions.appendExt(pointPropsEl, col.getKey(), new Text(row[col.getValue()]));
                }

                segment.addPoint(WayPoint.builder()
                        .lat(line._2._1())
                        .lon(line._2._2())
                        .time(line._1._2.longValue())
                        .cmt(trackId) // cache track segment ID in GPX 'comment'
                        .extensions(pointProps)
                        .build()
                );
            }

            for (n = 0; n < useridCount; n++) {
                Text userid = userids[n];
                List<TrackSegment.Builder> r = (List<TrackSegment.Builder>) result[n];

                Track.Builder trk = Track.builder();
                trk.segments(r.stream().map(TrackSegment.Builder::build).collect(Collectors.toList()));

                Document trkProps = b.newDocument();
                Element trkPropsEl = GPXExtensions.getOrCreate(trkProps);
                GPXExtensions.appendExt(trkPropsEl, GEN_USERID, userid.toString());
                trk.extensions(trkProps);

                result[n] = trk.build();
            }

            return Arrays.stream(result).map(t -> (Track) t).iterator();
        }, true);

        return Collections.singletonMap(outputName, output);
    }
}
