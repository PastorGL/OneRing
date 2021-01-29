/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.*;

import java.util.*;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class PolygonStatsOperation extends Operation {
    public static final String VERB = "polygonStats";

    private String inputName;
    private String outputName;

    @Override
    @Description("Take a Polygon RDD and augment its properties with statistics")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Polygon},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Polygon},
                                new String[]{GEN_PERIMETER, GEN_AREA}
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);
    }

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
