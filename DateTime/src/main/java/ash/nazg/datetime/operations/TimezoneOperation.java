/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.datetime.config.ConfigurationParameters;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.io.StringWriter;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

@SuppressWarnings("unused")
public class TimezoneOperation extends Operation {
    @Description("Generated full input date column")
    public static final String GEN_INPUT_DATE = "_input_date";
    @Description("Generated input day of week column")
    public static final String GEN_INPUT_DOW_INT = "_input_dow_int";
    @Description("Generated input date of month column")
    public static final String GEN_INPUT_DAY_INT = "_input_day_int";
    @Description("Generated input month column")
    public static final String GEN_INPUT_MONTH_INT = "_input_month_int";
    @Description("Generated input year column")
    public static final String GEN_INPUT_YEAR_INT = "_input_year_int";
    @Description("Generated input hour column")
    public static final String GEN_INPUT_HOUR_INT = "_input_hour_int";
    @Description("Generated input minute column")
    public static final String GEN_INPUT_MINUTE_INT = "_input_minute_int";
    @Description("Generated full output date column")
    public static final String GEN_OUTPUT_DATE = "_output_date";
    @Description("Generated output day of week column")
    public static final String GEN_OUTPUT_DOW_INT = "_output_dow_int";
    @Description("Generated output date of month column")
    public static final String GEN_OUTPUT_DAY_INT = "_output_day_int";
    @Description("Generated output month column")
    public static final String GEN_OUTPUT_MONTH_INT = "_output_month_int";
    @Description("Generated output year column")
    public static final String GEN_OUTPUT_YEAR_INT = "_output_year_int";
    @Description("Generated output hour column")
    public static final String GEN_OUTPUT_HOUR_INT = "_output_hour_int";
    @Description("Generated output minute column")
    public static final String GEN_OUTPUT_MINUTE_INT = "_output_minute_int";
    @Description("Generated Epoch time of the timestamp")
    public static final String GEN_EPOCH_TIME = "_epoch_time";
    @Description("By default, source time zone is GMT")
    public static final String DEF_SRC_TIMEZONE_DEFAULT = "GMT";
    @Description("By default, destination time zone is GMT")
    public static final String DEF_DST_TIMEZONE_DEFAULT = "GMT";
    @Description("By default, use ISO formatting for the full source date")
    public static final String DEF_SRC_TIMESTAMP_FORMAT = null;
    @Description("By default, use ISO formatting for the full destination date")
    public static final String DEF_DST_TIMESTAMP_FORMAT = null;
    @Description("By default, do not read source time zone from input column")
    public static final String DEF_SRC_TIMEZONE_COL = null;
    @Description("By default, do not read destination time zone from input column")
    public static final String DEF_DST_TIMEZONE_COL = null;

    public static final String VERB = "timezone";

    private String inputName, outputName;

    private String timestampColumn;
    private String timezoneColumn;
    private String outputTimezoneColumn;

    private Map<String, Integer> inputColumns;
    private char inputDelimiter;
    private String[] outputColumns;
    private char outputDelimiter;

    private String timestampFormat;
    private String outputTimestampFormat;
    private TimeZone sourceTimezoneDefault;
    private TimeZone destinationTimezoneDefault;

    @Override
    @Description("Take a CSV RDD with a timestamp column (Epoch seconds or milliseconds, ISO of custom format) and explode" +
            " timestamp components into individual columns. Perform timezone conversion, using source and destination" +
            " timezones from the parameters or another source columns")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_SRC_TIMESTAMP_COLUMN),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_SRC_TIMESTAMP_FORMAT, DEF_SRC_TIMESTAMP_FORMAT),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_SRC_TIMEZONE_COL, DEF_SRC_TIMEZONE_COL),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_SRC_TIMEZONE_DEFAULT, DEF_SRC_TIMEZONE_DEFAULT),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_DST_TIMESTAMP_FORMAT, DEF_DST_TIMESTAMP_FORMAT),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_DST_TIMEZONE_COL, DEF_DST_TIMEZONE_COL),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_DST_TIMEZONE_DEFAULT, DEF_DST_TIMEZONE_DEFAULT),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.CSV},
                                new String[]{
                                        GEN_INPUT_DATE, GEN_INPUT_DOW_INT, GEN_INPUT_DAY_INT, GEN_INPUT_MONTH_INT, GEN_INPUT_YEAR_INT, GEN_INPUT_HOUR_INT, GEN_INPUT_MINUTE_INT,
                                        GEN_OUTPUT_DATE, GEN_OUTPUT_DOW_INT, GEN_OUTPUT_DAY_INT, GEN_OUTPUT_MONTH_INT, GEN_OUTPUT_YEAR_INT, GEN_OUTPUT_HOUR_INT, GEN_OUTPUT_MINUTE_INT,
                                        GEN_EPOCH_TIME
                                }
                        )
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputColumns = dsResolver.inputColumns(inputName);
        inputDelimiter = dsResolver.inputDelimiter(inputName);

        timestampColumn = opResolver.definition(ConfigurationParameters.DS_SRC_TIMESTAMP_COLUMN);
        timezoneColumn = opResolver.definition(ConfigurationParameters.OP_SRC_TIMEZONE_COL);
        if (timezoneColumn == null) {
            String timezoneDefault = opResolver.definition(ConfigurationParameters.OP_SRC_TIMEZONE_DEFAULT);
            sourceTimezoneDefault = TimeZone.getTimeZone(timezoneDefault);
        }
        timestampFormat = opResolver.definition(ConfigurationParameters.OP_SRC_TIMESTAMP_FORMAT);

        outputName = opResolver.positionalOutput(0);
        outputColumns = dsResolver.outputColumns(outputName);
        outputDelimiter = dsResolver.outputDelimiter(outputName);

        outputTimezoneColumn = opResolver.definition(ConfigurationParameters.OP_DST_TIMEZONE_COL);
        if (outputTimezoneColumn == null) {
            String outputTimezoneDefault = opResolver.definition(ConfigurationParameters.OP_DST_TIMEZONE_DEFAULT);
            destinationTimezoneDefault = TimeZone.getTimeZone(outputTimezoneDefault);
        }
        outputTimestampFormat = opResolver.definition(ConfigurationParameters.OP_DST_TIMESTAMP_FORMAT);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final Map<String, Integer> _inputColumns = inputColumns;
        final char _inputDelimiter = inputDelimiter;
        final String[] _outputColumns = outputColumns;
        final char _outputDelimiter = outputDelimiter;

        final int _sourceTimestampColumn = inputColumns.get(timestampColumn);
        final Integer _sourceTimezoneColumn = inputColumns.get(timezoneColumn);
        final TimeZone _sourceTimezoneDefault = sourceTimezoneDefault;
        final String _sourceTimestampFormat = timestampFormat;

        final Integer _destinationTimezoneColumn = inputColumns.get(outputTimezoneColumn);
        final TimeZone _destinationTimezoneDefault = destinationTimezoneDefault;
        final String _destinationTimestampFormat = outputTimestampFormat;

        JavaRDD<Object> signalsInput = (JavaRDD<Object>) input.get(inputName);

        JavaRDD<Text> signals = signalsInput
                .mapPartitions(it -> {
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

                    ZoneId GMT = TimeZone.getTimeZone(DEF_SRC_TIMEZONE_DEFAULT).toZoneId();

                    DateTimeFormatter dtfInput = (_sourceTimestampFormat != null)
                            ? new DateTimeFormatterBuilder().appendPattern(_sourceTimestampFormat).toFormatter()
                            : null;
                    DateTimeFormatter dtfOutput = (_destinationTimestampFormat != null)
                            ? new DateTimeFormatterBuilder().appendPattern(_destinationTimestampFormat).toFormatter()
                            : null;

                    List<Text> result = new ArrayList<>();
                    while (it.hasNext()) {
                        Object line = it.next();

                        Map<String, String> properties = new HashMap<>();

                        String l;
                        if (line instanceof String) {
                            l = (String) line;
                        } else {
                            l = String.valueOf(line);
                        }
                        String[] row = parser.parseLine(l);

                        _inputColumns.forEach((k, v) -> properties.put(k, row[v]));

                        long timestamp;
                        String timestampText = row[_sourceTimestampColumn];

                        ZoneId inputTimezone = (_sourceTimezoneColumn == null)
                                ? _sourceTimezoneDefault.toZoneId()
                                : TimeZone.getTimeZone(row[_sourceTimezoneColumn]).toZoneId();

                        if (dtfInput != null) {
                            timestamp = Date.from(Instant.from(dtfInput.withZone(inputTimezone).parse(timestampText))).getTime();
                        } else try {
                            // timestamp is in milliseconds
                            timestamp = new Double(timestampText).longValue();

                            // timestamp is in seconds
                            if (timestamp < 100_000_000_000L) {
                                timestamp *= 1000L;
                            }
                        } catch (NumberFormatException e) {
                            // timestamp is ISO
                            timestamp = Date.from(Instant.parse(timestampText)).getTime();
                        } // fail otherwise

                        LocalDateTime localGMTDate = LocalDateTime.ofInstant(new Date(timestamp).toInstant(), GMT);

                        ZonedDateTime inputDate = ZonedDateTime.of(
                                localGMTDate.getYear(),
                                localGMTDate.getMonth().getValue(),
                                localGMTDate.getDayOfMonth(),
                                localGMTDate.getHour(),
                                localGMTDate.getMinute(),
                                localGMTDate.getSecond(),
                                localGMTDate.getNano(),
                                inputTimezone
                        );

                        ZoneId outputTimezone = (_destinationTimezoneColumn == null)
                                ? _destinationTimezoneDefault.toZoneId()
                                : TimeZone.getTimeZone(row[_destinationTimezoneColumn]).toZoneId();

                        ZonedDateTime outputDate = inputDate.toInstant().atZone(outputTimezone);

                        properties.put(GEN_INPUT_DATE, (dtfOutput != null) ? dtfOutput.withZone(inputTimezone).format(inputDate) : inputDate.toString());
                        properties.put(GEN_INPUT_DOW_INT, String.valueOf(inputDate.getDayOfWeek().getValue()));
                        properties.put(GEN_INPUT_DAY_INT, String.valueOf(inputDate.getDayOfMonth()));
                        properties.put(GEN_INPUT_MONTH_INT, String.valueOf(inputDate.getMonthValue()));
                        properties.put(GEN_INPUT_YEAR_INT, String.valueOf(inputDate.getYear()));
                        properties.put(GEN_INPUT_HOUR_INT, String.valueOf(inputDate.getHour()));
                        properties.put(GEN_INPUT_MINUTE_INT, String.valueOf(inputDate.getMinute()));
                        properties.put(GEN_OUTPUT_DATE, (dtfOutput != null) ? dtfOutput.withZone(outputTimezone).format(outputDate) : outputDate.toString());
                        properties.put(GEN_OUTPUT_DOW_INT, String.valueOf(outputDate.getDayOfWeek().getValue()));
                        properties.put(GEN_OUTPUT_DAY_INT, String.valueOf(outputDate.getDayOfMonth()));
                        properties.put(GEN_OUTPUT_MONTH_INT, String.valueOf(outputDate.getMonthValue()));
                        properties.put(GEN_OUTPUT_YEAR_INT, String.valueOf(outputDate.getYear()));
                        properties.put(GEN_OUTPUT_HOUR_INT, String.valueOf(outputDate.getHour()));
                        properties.put(GEN_OUTPUT_MINUTE_INT, String.valueOf(outputDate.getMinute()));
                        properties.put(GEN_EPOCH_TIME, String.valueOf(localGMTDate.toEpochSecond(ZoneOffset.UTC)));

                        StringWriter buffer = new StringWriter();
                        String[] acc = new String[_outputColumns.length];

                        for (int i = 0; i < _outputColumns.length; i++) {
                            acc[i] = properties.get(_outputColumns[i]);
                        }

                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(acc, false);
                        writer.close();

                        result.add(new Text(buffer.toString()));
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, signals);
    }
}
