package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import ash.nazg.spatial.TrackSegment;
import ash.nazg.spatial.functions.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.hadoop.io.MapWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.*;

import static ash.nazg.config.tdl.TaskDescriptionLanguage.StreamType.*;

@SuppressWarnings("unused")
public class SpatialToolboxOperation extends Operation {
    private static final String VERB = "spatialToolbox";

    @Description("Selector for properties, SQL-like")
    public static final String OP_QUERY = "query";

    private String outputName;
    private String inputName;
    private List<Expressions.QueryExpr> query;
    private String what;
    private Long limitRecords;
    private Double limitPercent;

    @Description("This operation allows SELECT queries against any Spatially-typed RDDs using any of their properties" +
            " as criteria, e.g. SELECT Point FROM tracks WHERE trackid LIKE '.+?non.*' OR pt = 'e2e'")
    @Override
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(VERB,
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_QUERY)
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{Point, Track, Polygon},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{Passthru},
                                false
                        )
                ));
    }

    @Override
    public void configure(Properties config, Properties variables) throws InvalidConfigValueException {
        super.configure(config, variables);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);

        String queryString = describedProps.defs.getTyped(OP_QUERY);
        CharStream cs = CharStreams.fromString(queryString);

        QueryLexer lexer = new QueryLexer(cs);
        QueryParser parser = new QueryParser(new CommonTokenStream(lexer));
        QueryListenerImpl listener = new QueryListenerImpl();
        parser.addParseListener(listener);
        parser.parse();

        query = listener.getQuery();
        what = listener.getWhat().get(0);
        if (!Arrays.asList("Point", "Polygon", "TrackSegment", "SegmentedTrack").contains(what)) {
            throw new InvalidConfigValueException("Unknown spatial object type '" + what + "' to SELECT in operation '" + name + "'");
        }

        limitRecords = listener.getLimitRecords();
        limitPercent = listener.getLimitPercent();
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final List<Expressions.QueryExpr> _query = query;
        final String _what = what;

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<Object> output = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                    List<Object> ret = new ArrayList<>();

                    QueryMatcher qm = new QueryMatcher(_query);
                    boolean selectTrackSegment = _what.equalsIgnoreCase("TrackSegment");

                    while (it.hasNext()) {
                        Geometry g = (Geometry) it.next();
                        String thisType = g.getGeometryType();

                        MapWritable props = (MapWritable) g.getUserData();

                        if (thisType.equalsIgnoreCase(_what)) { // direct SELECT of Point or Polygon or SegmentedTrack
                            if (qm.matches(props)) {
                                ret.add(g);
                            }
                        } else { // otherwise SELECTing objects inside SegmentedTrack
                            List<TrackSegment> segments = new ArrayList<>();

                            int numSegments = g.getNumGeometries();
                            for (int n = 0; n < numSegments; n++) {
                                TrackSegment seg = (TrackSegment) g.getGeometryN(n);

                                MapWritable segProps = (MapWritable) seg.getUserData();
                                MapWritable props2 = new MapWritable(props);
                                props2.putAll(segProps);

                                if (selectTrackSegment) { // SELECTing TrackSegments
                                    if (qm.matches(props2)) {
                                        segments.add(seg);
                                    }
                                } else { // SELECTing Points
                                    List<Geometry> points = new ArrayList<>();

                                    int numPoints = seg.getNumGeometries();
                                    for (int nn = 0; nn < numPoints; nn++) {
                                        Geometry point = seg.getGeometryN(nn);

                                        MapWritable pointProps = (MapWritable) point.getUserData();
                                        MapWritable props3 = new MapWritable(props2);
                                        props3.putAll(pointProps);

                                        if (qm.matches(props3)) {
                                            points.add(point);
                                        }
                                    }

                                    if (!points.isEmpty()) {
                                        TrackSegment newSeg = new TrackSegment(points.toArray(new Geometry[0]), geometryFactory);
                                        newSeg.setUserData(segProps);

                                        segments.add(newSeg);
                                    }
                                }
                            }

                            if (!segments.isEmpty()) {
                                SegmentedTrack track = new SegmentedTrack(segments.toArray(new Geometry[0]), geometryFactory);
                                track.setUserData(props);

                                ret.add(track);
                            }
                        }
                    }

                    return ret.iterator();
                });

        if (limitRecords != null) {
            output = output.sample(false, limitRecords.doubleValue() / output.count());
        }
        if (limitPercent != null) {
            output = output.sample(false, limitPercent);
        }

        return Collections.singletonMap(outputName, output);
    }

    private static class QueryMatcher implements Serializable {
        private final List<Expressions.QueryExpr> query;

        public QueryMatcher(List<Expressions.QueryExpr> query) {
            this.query = query;
        }

        public boolean matches(MapWritable props) {
            List<Boolean> stack = new ArrayList<>();

            if (query.isEmpty()) {
                return true;
            }

            String prop = null;
            List<Boolean> stackPortion = null;
            for (Expressions.QueryExpr qe : query) {
                if (qe instanceof Expressions.PropGetter) {
                    Object o = ((Expressions.PropGetter) qe).get(props);
                    prop = (o == null) ? null : o.toString();
                    continue;
                }
                if (qe instanceof Expressions.StackGetter) {
                    stackPortion = ((Expressions.StackGetter) qe).eval(stack);
                    continue;
                }
                if (qe instanceof Expressions.StringExpr) {
                    stack.add(((Expressions.StringExpr) qe).eval(prop));
                    continue;
                }
                if (qe instanceof Expressions.NumericExpr) {
                    stack.add((prop != null) && ((Expressions.NumericExpr) qe).eval(new Double(prop)));
                    continue;
                }
                if (qe instanceof Expressions.LogicBinaryExpr) {
                    stack.add(((Expressions.LogicBinaryExpr) qe).eval(stackPortion.get(0), stackPortion.get(1)));
                    continue;
                }
                if (qe instanceof Expressions.LogicUnaryExpr) {
                    stack.add(((Expressions.LogicUnaryExpr) qe).eval(stackPortion.get(0)));
                    continue;
                }
                if (qe instanceof Expressions.NullExpr) {
                    stack.add(((Expressions.NullExpr) qe).eval(prop));
                }
            }

            return stack.get(stack.size() - 1);
        }
    }
}
