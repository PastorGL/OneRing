/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.config.ConfigurationParameters;
import ash.nazg.spatial.functions.PointGeoJSONMapper;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class PointJSONSourceOperation extends Operation {
    @Description("By default, don't add _radius attribute to the point")
    public static final Double DEF_DEFAULT_RADIUS = null;

    public static final String VERB = "pointJsonSource";

    private String inputName;

    private String outputName;
    private List<String> outputColumns;
    private Double defaultRadius;

    @Override
    @Description("Take GeoJSON fragment file and produce a Point RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_DEFAULT_RADIUS, Double.class, DEF_DEFAULT_RADIUS),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
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
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);

        inputName = describedProps.inputs.get(0);

        outputName = describedProps.outputs.get(0);
        outputColumns = Arrays.asList(dataStreamsProps.outputColumns.get(outputName));
        defaultRadius = describedProps.defs.getTyped(ConfigurationParameters.OP_DEFAULT_RADIUS);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDDLike rdd = input.get(inputName);

        return Collections.singletonMap(outputName, rdd.flatMap(new PointGeoJSONMapper(defaultRadius, outputColumns)));
    }
}
