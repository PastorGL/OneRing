/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.metadata.OperationMeta;
import ash.nazg.metadata.Origin;
import ash.nazg.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.data.DataStream;
import ash.nazg.data.StreamType;
import ash.nazg.scripting.Operation;
import ash.nazg.data.spatial.PolygonEx;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class PolygonStatsOperation extends Operation {
    private static final String GEN_AREA = "_area";
    private static final String GEN_PERIMETER = "_perimeter";
    private static final String GEN_VERTICES = "_vertices";
    private static final String GEN_HOLES = "_holes";

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonStats", "Take a Polygon DataStream and augment its properties with statistics",

                new PositionalStreamsMetaBuilder()
                        .input("Polygon RDD to count the stats",
                                new StreamType[]{StreamType.Polygon}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .output("Polygon RDD with stat parameters calculated",
                                new StreamType[]{StreamType.Polygon}, Origin.AUGMENTED, null
                        )
                        .generated(GEN_HOLES, "Number of Polygon holes")
                        .generated(GEN_PERIMETER, "Polygon perimeter in meters")
                        .generated(GEN_VERTICES, "Number of Polygon vertices")
                        .generated(GEN_AREA, "Polygon area in square meters")
                        .build()
        );
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        DataStream input = inputStreams.getValue(0);
        JavaRDD<PolygonEx> output = ((JavaRDD<PolygonEx>) input.get())
                .mapPartitions(it -> {
                    List<PolygonEx> result = new ArrayList<>();

                    PolygonArea pArea = new PolygonArea(Geodesic.WGS84, false);

                    while (it.hasNext()) {
                        PolygonEx next = it.next();

                        pArea.Clear();
                        for (Coordinate c : next.getExteriorRing().getCoordinates()) {
                            pArea.AddPoint(c.y, c.x);
                        }

                        PolygonResult pRes = pArea.Compute();

                        int numHoles = next.getNumInteriorRing();
                        next.put(GEN_HOLES, numHoles);
                        next.put(GEN_PERIMETER, pRes.perimeter);
                        next.put(GEN_VERTICES, pRes.num);

                        double area = Math.abs(pRes.area);
                        for (int hole = numHoles; hole > 0; hole--) {
                            LineString lr = next.getInteriorRingN(hole - 1);

                            pArea.Clear();
                            for (Coordinate c : lr.getCoordinates()) {
                                pArea.AddPoint(c.y, c.x);
                            }

                            area -= Math.abs(pArea.Compute().area);
                        }
                        next.put(GEN_AREA, area);

                        result.add(next);
                    }

                    return result.iterator();
                });

        List<String> polygonProps = input.accessor.attributes("polygon");
        List<String> outputColumns = (polygonProps == null) ? new ArrayList<>() : new ArrayList<>(polygonProps);
        outputColumns.add(GEN_HOLES);
        outputColumns.add(GEN_PERIMETER);
        outputColumns.add(GEN_VERTICES);
        outputColumns.add(GEN_AREA);
        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Polygon, output, Collections.singletonMap("polygon", outputColumns)));
    }
}
