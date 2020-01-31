/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.config.ConfigurationParameters;
import ash.nazg.spatial.functions.PointCSVMapper;
import ash.nazg.config.OperationConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class PointCSVSourceOperation extends Operation {
    @Description("By default, don't set Point _radius attribute")
    public static final Double DEF_DEFAULT_RADIUS = null;
    @Description("By default, don't set Point _radius attribute")
    public static final String DEF_CSV_RADIUS_COLUMN = null;

    public static final String VERB = "pointCsvSource";

    private String inputName;
    private char inputDelimiter;
    private Map<String, Integer> inputColumns;
    private int latColumn;
    private int lonColumn;

    private Integer radiusColumn;
    private Double defaultRadius;

    private String outputName;
    private List<String> outputColumns;

    @Override
    @Description("Take a CSV file and produce a Polygon RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_DEFAULT_RADIUS, Double.class, DEF_DEFAULT_RADIUS),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_CSV_RADIUS_COLUMN, DEF_CSV_RADIUS_COLUMN),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_CSV_LAT_COLUMN),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_CSV_LON_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
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

        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);

        defaultRadius = describedProps.defs.getTyped(ConfigurationParameters.OP_DEFAULT_RADIUS);

        inputColumns = dataStreamsProps.inputColumns.get(inputName);
        outputColumns = Arrays.asList(dataStreamsProps.outputColumns.get(outputName));

        String prop;

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CSV_RADIUS_COLUMN);
        radiusColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CSV_LAT_COLUMN);
        latColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CSV_LON_COLUMN);
        lonColumn = inputColumns.get(prop);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDDLike rdd = input.get(inputName);

        JavaRDD javaRDD = rdd.mapPartitions(new PointCSVMapper(inputColumns, latColumn, lonColumn, radiusColumn, defaultRadius, inputDelimiter, outputColumns));
        return Collections.singletonMap(outputName, javaRDD);
    }
}
