package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import io.jenetics.jpx.GPX;
import io.jenetics.jpx.Track;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

@SuppressWarnings("unused")
public class TrackGPXOutputOperation extends Operation {
    public static final String VERB = "trackGpxOutput";

    private String inputName;
    private String outputName;

    @Override
    @Description("Take a Track RDD and produce a GPX fragment file")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                false
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
        JavaRDD<Text> output = ((JavaRDD<Track>) input.get(inputName))
                .mapPartitions(it -> {
                    List<Text> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Track trk = it.next();

                        GPX.Writer writer = GPX.writer();
                        GPX.Builder gpx = GPX.builder();
                        gpx.creator("OneRing");
                        gpx.addTrack(trk);

                        result.add(new Text(writer.toString(gpx.build())));
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
