/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.commons.functions.*;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import ash.nazg.spatial.TrackSegment;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.*;
import java.util.function.BiFunction;

import static ash.nazg.config.tdl.StreamType.*;

@SuppressWarnings("unused")
public class SpatialToolboxOperation extends Operation {
    public static final String OP_QUERY = "query";

    private String inputName;

    private String outputName;

    private List<Expressions.ExprItem<?>> query;
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

        QueryLexicon lexer = new QueryLexicon(cs);
        QueryParser parser = new QueryParser(new CommonTokenStream(lexer));
        QueryListenerImpl listener = new QueryListenerImpl();
        parser.addParseListener(listener);
        parser.parse();

        query = listener.getQuery();
        Expressions.ExprItem<?> exprItem = listener.getItems().get(0).get(0);
        if (!(exprItem instanceof Expressions.SpatialItem) || listener.getItems().size() != 1) {
            throw new InvalidConfigValueException("Operation '" + name + "' supports only spatial type queries");
        }

        what = ((Expressions.SpatialItem) exprItem).get();
        if (!Arrays.asList("Point", "Polygon", "TrackSegment", "SegmentedTrack").contains(what)) {
            throw new InvalidConfigValueException("Unknown spatial object type '" + what + "' to SELECT in operation '" + name + "'");
        }

        limitRecords = listener.getLimitRecords();
        limitPercent = listener.getLimitPercent();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final List<Expressions.ExprItem<?>> _query = query;
        final String _what = what;

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<Object> output = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                    List<Object> ret = new ArrayList<>();

                    boolean selectTrackSegment = _what.equalsIgnoreCase("TrackSegment");

                    BiFunction<Object, String, Object> propGetter = (Object props, String prop) -> String.valueOf(((MapWritable) props).get(new Text(prop)));

                    while (it.hasNext()) {
                        Geometry g = (Geometry) it.next();
                        String thisType = g.getGeometryType();

                        MapWritable props = (MapWritable) g.getUserData();

                        if (thisType.equalsIgnoreCase(_what)) { // direct SELECT of Point or Polygon or SegmentedTrack
                            if (Operator.bool(propGetter, props, null, _query)) {
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
                                    if (Operator.bool(propGetter, props2, null, _query)) {
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

                                        if (Operator.bool(propGetter, props3, null, _query)) {
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
}
