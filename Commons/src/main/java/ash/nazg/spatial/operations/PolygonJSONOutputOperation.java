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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    public void setConfig(OperationConfig config) throws InvalidConfigValueException {
        super.setConfig(config);

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
