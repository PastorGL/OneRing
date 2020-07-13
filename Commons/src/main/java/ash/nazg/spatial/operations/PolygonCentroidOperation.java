/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.*;

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_CENTER_LAT;
import static ash.nazg.spatial.config.ConfigurationParameters.GEN_CENTER_LON;

@SuppressWarnings("unused")
public class PolygonCentroidOperation extends Operation {
    public static final String VERB = "polygonCentroid";

    private String inputName;
    private String outputName;

    @Override
    @Description("Take a Polygon RDD and extract a Point RDD of centroids while keeping all properties")
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
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
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
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Point> output = ((JavaRDD<Polygon>) input.get(inputName))
                .mapPartitions(it -> {
                    Text latAttr = new Text(GEN_CENTER_LAT);
                    Text lonAttr = new Text(GEN_CENTER_LON);

                    List<Point> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Polygon next = it.next();

                        Point centroid = next.getCentroid();
                        Point point = (Point) centroid.copy();

                        MapWritable props = new MapWritable();
                        props.putAll((MapWritable) next.getUserData());
                        props.put(latAttr, new DoubleWritable(centroid.getY()));
                        props.put(lonAttr, new DoubleWritable(centroid.getX()));
                        point.setUserData(props);

                        result.add(point);
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
