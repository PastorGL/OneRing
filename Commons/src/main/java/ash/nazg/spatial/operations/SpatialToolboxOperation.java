/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import ash.nazg.spatial.TrackSegment;
import ash.nazg.spatial.functions.Expressions;
import ash.nazg.spatial.functions.QueryLexer;
import ash.nazg.spatial.functions.QueryListenerImpl;
import ash.nazg.spatial.functions.QueryParser;
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

import static ash.nazg.config.tdl.StreamType.*;

@SuppressWarnings("unused")
public class SpatialToolboxOperation extends Operation {
    public static final String OP_QUERY = "query";

    private String inputName;

    private String outputName;

    private List<Expressions.QueryExpr> query;
    private String what;
    private Long limitRecords;
    private Double limitPercent;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("spatialToolbox", "This operation allows SELECT queries against any Spatially-typed RDDs using any of their properties" +
                " as criteria, e.g. SELECT Point FROM tracks WHERE trackid LIKE '.+?non.*' OR pt = 'e2e'",

                new PositionalStreamsMetaBuilder()
                        .ds("Any Geometry-type RDD",
                                new StreamType[]{Point, Track, Polygon}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_QUERY, "Query for object properties, SQL SELECT-like")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Same type of input, with objects adhering to a query",
                                new StreamType[]{Passthru}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);

        String queryString = opResolver.definition(OP_QUERY);
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

    @SuppressWarnings("rawtypes")
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
            if (query.isEmpty()) {
                return true;
            }

            Deque<Boolean> stack = new LinkedList<>();
            Deque<Boolean> top = null;
            Object prop = null;
            for (Expressions.QueryExpr qe : query) {
                if (qe instanceof Expressions.PropGetter) {
                    prop = ((Expressions.PropGetter) qe).get(props);
                    continue;
                }
                if (qe instanceof Expressions.StackGetter) {
                    top = ((Expressions.StackGetter) qe).eval(stack);
                    continue;
                }
                if (qe instanceof Expressions.StringExpr) {
                    stack.push(((Expressions.StringExpr) qe).eval((String) prop));
                    continue;
                }
                if (qe instanceof Expressions.NumericExpr) {
                    stack.push(((Expressions.NumericExpr) qe).eval((Double) prop));
                    continue;
                }
                if (qe instanceof Expressions.LogicBinaryExpr) {
                    stack.push(((Expressions.LogicBinaryExpr) qe).eval(top.pop(), top.pop()));
                    continue;
                }
                if (qe instanceof Expressions.LogicUnaryExpr) {
                    stack.push(((Expressions.LogicUnaryExpr) qe).eval(top.pop()));
                    continue;
                }
                if (qe instanceof Expressions.NullExpr) {
                    stack.push(((Expressions.NullExpr) qe).eval(prop));
                }
            }

            return stack.pop();
        }
    }
}
