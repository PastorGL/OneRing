/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Polygon;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.util.*;

@SuppressWarnings("unused")
public class PolygonJSONOutputOperation extends Operation {
    public static final String VERB = "polygonJsonOutput";

    private String inputName;
    private String outputName;

    @Override
    @Description("Take a Polygon RDD and produce a GeoJSON fragment file")
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
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
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
        JavaRDD<Text> output = ((JavaRDD<Polygon>) input.get(inputName))
                .mapPartitions(it -> {
                    GeoJSONWriter wr = new GeoJSONWriter();

                    List<Text> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Polygon poly = it.next();
                        result.add(new Text(wr.write(poly).toString()));
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
