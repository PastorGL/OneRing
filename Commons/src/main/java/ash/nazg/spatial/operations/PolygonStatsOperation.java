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
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class PolygonStatsOperation extends Operation {
    private static final String GEN_AREA = "_area";
    private static final String GEN_PERIMETER = "_perimeter";

    private String inputName;

    private String outputName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonStats", "Take a Polygon RDD and augment its properties with statistics",

                new PositionalStreamsMetaBuilder()
                        .ds("Polygon RDD to count the stats",
                                new StreamType[]{StreamType.Polygon}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("Polygon RDD with stat parameters calculated",
                                new StreamType[]{StreamType.Polygon}, true
                        )
                        .genCol(GEN_AREA, "Polygon area in square meters")
                        .genCol(GEN_PERIMETER, "Polygon perimeter in meters")
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
        JavaRDD<Polygon> output = ((JavaRDD<Polygon>) input.get(inputName))
                .mapPartitions(it -> {
                    Text areaAttr = new Text(GEN_AREA);
                    Text perimeterAttr = new Text(GEN_PERIMETER);

                    List<Polygon> result = new ArrayList<>();

                    PolygonArea pArea = new PolygonArea(Geodesic.WGS84, false);

                    while (it.hasNext()) {
                        Polygon next = it.next();

                        MapWritable props = (MapWritable) next.getUserData();

                        pArea.Clear();
                        for (Coordinate c : next.getExteriorRing().getCoordinates()) {
                            pArea.AddPoint(c.y, c.x);
                        }

                        PolygonResult pRes = pArea.Compute();

                        double perimeter = pRes.perimeter;
                        props.put(perimeterAttr, new DoubleWritable(perimeter));

                        double area = Math.abs(pRes.area);
                        for (int hole = next.getNumInteriorRing(); hole > 0; hole--) {
                            LineString lr = next.getInteriorRingN(hole - 1);

                            pArea.Clear();
                            for (Coordinate c : lr.getCoordinates()) {
                                pArea.AddPoint(c.y, c.x);
                            }

                            area -= Math.abs(pArea.Compute().area);
                        }
                        props.put(areaAttr, new DoubleWritable(area));

                        result.add(next);
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
