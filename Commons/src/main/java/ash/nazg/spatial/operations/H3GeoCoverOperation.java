package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import ash.nazg.spatial.SpatialUtils;
import com.opencsv.CSVWriter;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.util.GeoCoord;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_RADIUS;

@SuppressWarnings("unused")
public class H3GeoCoverOperation extends Operation {
    @Description("Default hash level")
    public static final Integer DEF_HASH_LEVEL = 9;
    @Description("Level of the hash")
    public static final String OP_HASH_LEVEL = "hash.level";
    @Description("Column with a generated hash value")
    public static final String GEN_HASH = "_hash";

    private static final String VERB = "h3GeoCover";

    protected Integer level;
    private String inputName;
    private String outputName;
    private char outputDelimiter;
    private List<String> outputColumns;

    @Override
    @Description("Create a complete (non-collapsed) H3 coverage from the Polygon or Point RDD. Can pass" +
            " any properties from the source geometries to the resulting CSV RDD columns, for each hash per each geometry")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_HASH_LEVEL, Integer.class, DEF_HASH_LEVEL),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point, TaskDescriptionLanguage.StreamType.Polygon},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                new String[]{GEN_HASH}
                        )
                )
        );

    }

    @Override
    public void setConfig(OperationConfig config) throws InvalidConfigValueException {
        super.setConfig(config);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);
        outputDelimiter = dataStreamsProps.outputDelimiter(outputName);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);
        String prop;

        List<Integer> out = new ArrayList<>();
        outputColumns = Arrays.stream(dataStreamsProps.outputColumns.get(outputName))
                .map(c -> c.replaceFirst("^" + inputName + "\\.", ""))
                .collect(Collectors.toList());

        level = describedProps.defs.getTyped(OP_HASH_LEVEL);

        if ((level < 0) || (level > 15)) {
            throw new InvalidConfigValueException("Mesh level must fall into interval '0'..'15' but is '" + level + "' in the operation '" + name + "'");
        }
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Object> geometriesInput = (JavaRDD<Object>) input.get(inputName);

        final List<String> _outputColumns = outputColumns;
        final char _outputDelimiter = outputDelimiter;
        int _level = level;

        JavaRDD<Text> output = geometriesInput.mapPartitions(it -> {
            Set<Text> ret = new HashSet<>();

            Text radiusAttr = new Text(GEN_RADIUS);

            H3Core h3 = H3Core.newInstance();
            final SpatialUtils spatialUtils = new SpatialUtils();

            while (it.hasNext()) {
                Object o = it.next();

                Geometry geometry = (Geometry) o;
                MapWritable props = (MapWritable) geometry.getUserData();

                Set<Long> polyfill = new HashSet<>();

                if (geometry instanceof Polygon) {
                    Polygon p = (Polygon) geometry;

                    List<GeoCoord> gco = new ArrayList<>();
                    for (Coordinate c : p.getExteriorRing().getCoordinates()) {
                        gco.add(new GeoCoord(c.y, c.x));
                    }

                    List<List<GeoCoord>> gci = new ArrayList<>();
                    for (int i = p.getNumInteriorRing(); i > 0; ) {
                        List<GeoCoord> gcii = new ArrayList<>();
                        for (Coordinate c : p.getInteriorRingN(--i).getCoordinates()) {
                            gcii.add(new GeoCoord(c.y, c.x));
                        }
                        gci.add(gcii);
                    }

                    polyfill.addAll(h3.polyfill(gco, gci, _level));
                }

                if (geometry instanceof Point) {
                    Coordinate c = geometry.getCoordinate();

                    long pointfill = h3.geoToH3(c.y, c.x, _level);
                    polyfill.add(pointfill);
                    if (props.containsKey(radiusAttr)) {
                        double radius = ((DoubleWritable) props.get(radiusAttr)).get();

                        int recursion = 1;
                        double length = h3.edgeLength(_level, LengthUnit.m);
                        if (radius > length) {
                            recursion = (int) Math.floor(radius / length);
                        }

                        polyfill.addAll(h3.kRing(pointfill, recursion));
                    }
                }

                for (Long hash : polyfill) {
                    String[] out = new String[_outputColumns.size()];

                    int i = 0;
                    for (String column : _outputColumns) {
                        if ("_hash".equals(column)) {
                            out[i++] = hash.toString();
                        } else {
                            out[i++] = props.get(new Text(column)).toString();
                        }
                    }

                    StringWriter buffer = new StringWriter();
                    CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                            CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                    writer.writeNext(out, false);
                    writer.close();

                    ret.add(new Text(buffer.toString()));
                }
            }

            return ret.iterator();
        });

        return Collections.singletonMap(outputName, output);
    }
}
