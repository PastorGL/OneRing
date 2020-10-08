package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import io.jenetics.jpx.GPX;
import io.jenetics.jpx.Track;
import org.apache.commons.codec.Charsets;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("unused")
public class TrackGPXSourceOperation extends Operation {
    public static final String VERB = "trackGpxSource";

    private String inputName;
    private String outputName;

    @Override
    @Description("Take GPX fragment file and produce a Track RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
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
        JavaRDD<Object> rdd = (JavaRDD<Object>) input.get(inputName);

        JavaRDD<Track> output = rdd.flatMap(o -> {
            String l = String.valueOf(o);

            GPX gpx = GPX.reader(GPX.Reader.Mode.LENIENT).read(new ByteArrayInputStream(l.getBytes(Charsets.UTF_8)));

            return gpx.getTracks().iterator();
        });

        return Collections.singletonMap(outputName, output);
    }
}
