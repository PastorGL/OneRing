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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.util.*;
import java.util.function.Function;

@SuppressWarnings("unused")
public class PolygonJSONOutputOperation extends Operation {
    private String inputName;

    private String outputName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonJsonOutput", "Take a Polygon RDD and produce a GeoJSON fragment RDD",

                new PositionalStreamsMetaBuilder()
                        .ds("Polygon RDD",
                                new StreamType[]{StreamType.Polygon}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("Plain RDD with GeoJSON fragment per input Polygon on each line",
                                new StreamType[]{StreamType.Plain}
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
        JavaRDD<Text> output = ((JavaRDD<Polygon>) input.get(inputName))
                .mapPartitions(it -> {
                    GeoJSONWriter wr = new GeoJSONWriter();

                    List<Text> result = new ArrayList<>();

                    Function<Coordinate[], double[][]> convert = (Coordinate[] coordinates) -> {
                        double[][] array = new double[coordinates.length][];
                        for (int i = 0; i < coordinates.length; i++) {
                            array[i] = new double[]{coordinates[i].x, coordinates[i].y};
                        }
                        return array;
                    };

                    while (it.hasNext()) {
                        Polygon poly = it.next();

                        int size = poly.getNumInteriorRing() + 1;
                        double[][][] rings = new double[size][][];
                        rings[0] = convert.apply(poly.getExteriorRing().getCoordinates());
                        for (int i = 0; i < size - 1; i++) {
                            rings[i + 1] = convert.apply(poly.getInteriorRingN(i).getCoordinates());
                        }

                        MapWritable props = (MapWritable) poly.getUserData();
                        Map<String, Object> featureProps = new HashMap<>();
                        props.forEach((k, v) -> featureProps.put(k.toString(), v.toString()));

                        result.add(new Text(new org.wololo.geojson.Feature(new org.wololo.geojson.Polygon(rings), featureProps).toString()));
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
