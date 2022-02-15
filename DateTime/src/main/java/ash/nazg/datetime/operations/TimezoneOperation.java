/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
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
    public static final String GEN_INPUT_DATE = "_input_date";
    public static final String GEN_INPUT_DOW_INT = "_input_dow_int";
    public static final String GEN_INPUT_DAY_INT = "_input_day_int";
    public static final String GEN_INPUT_MONTH_INT = "_input_month_int";
    public static final String GEN_INPUT_YEAR_INT = "_input_year_int";
    public static final String GEN_INPUT_HOUR_INT = "_input_hour_int";
    public static final String GEN_INPUT_MINUTE_INT = "_input_minute_int";
    public static final String GEN_OUTPUT_DATE = "_output_date";
    public static final String GEN_OUTPUT_DOW_INT = "_output_dow_int";
    public static final String GEN_OUTPUT_DAY_INT = "_output_day_int";
    public static final String GEN_OUTPUT_MONTH_INT = "_output_month_int";
    public static final String GEN_OUTPUT_YEAR_INT = "_output_year_int";
    public static final String GEN_OUTPUT_HOUR_INT = "_output_hour_int";
    public static final String GEN_OUTPUT_MINUTE_INT = "_output_minute_int";
    public static final String GEN_EPOCH_TIME = "_epoch_time";

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
    public OperationMeta meta() {
        return new OperationMeta("timezone", "Take a CSV RDD with a timestamp column (Epoch seconds or" +
                " milliseconds, ISO of custom format) and explode its components into individual columns." +
                " Perform timezone conversion, using source and destination timezones from the parameters or" +
                " another source columns",

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD with timestamp and optional timezone columns",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(ConfigurationParameters.DS_SRC_TIMESTAMP_COLUMN, "Source column with a timestamp")
                        .def(ConfigurationParameters.OP_SRC_TIMESTAMP_FORMAT, "If set, use this format to parse source timestamp", null, "By default, use ISO formatting for the full source date")
                        .def(ConfigurationParameters.OP_SRC_TIMEZONE_COL, "Source timezone default", null, "By default, do not read source time zone from input column")
                        .def(ConfigurationParameters.OP_SRC_TIMEZONE_DEFAULT, "If set, use source timezone from this column instead of the default", "GMT", "By default, source time zone is GMT")
                        .def(ConfigurationParameters.OP_DST_TIMESTAMP_FORMAT, "If set, use this format to output full date", null, "By default, use ISO formatting for the full destination date")
                        .def(ConfigurationParameters.OP_DST_TIMEZONE_COL, "Destination timezone default", null, "By default, do not read destination time zone from input column")
                        .def(ConfigurationParameters.OP_DST_TIMEZONE_DEFAULT, "If set, use destination timezone from this column instead of the default", "GMT", "By default, destination time zone is GMT")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD with exploded timestamp component columns",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .genCol(GEN_INPUT_DATE, "Generated full input date column")
                        .genCol(GEN_INPUT_DOW_INT, "Generated input day of week column")
                        .genCol(GEN_INPUT_DAY_INT, "Generated input date of month column")
                        .genCol(GEN_INPUT_MONTH_INT, "Generated input month column")
                        .genCol(GEN_INPUT_YEAR_INT, "Generated input year column")
                        .genCol(GEN_INPUT_HOUR_INT, "Generated input hour column")
                        .genCol(GEN_INPUT_MINUTE_INT, "Generated input minute column")
                        .genCol(GEN_OUTPUT_DATE, "Generated full output date column")
                        .genCol(GEN_OUTPUT_DOW_INT, "Generated output day of week column")
                        .genCol(GEN_OUTPUT_DAY_INT, "Generated output date of month column")
                        .genCol(GEN_OUTPUT_MONTH_INT, "Generated output month column")
                        .genCol(GEN_OUTPUT_YEAR_INT, "Generated output year column")
                        .genCol(GEN_OUTPUT_HOUR_INT, "Generated output hour column")
                        .genCol(GEN_OUTPUT_MINUTE_INT, "Generated output minute column")
                        .genCol(GEN_EPOCH_TIME, "Generated Epoch time of the timestamp")
                        .build()
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

                    ZoneId GMT = TimeZone.getTimeZone("GMT").toZoneId();

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
                            timestamp = new Double(Double.parseDouble(timestampText)).longValue();

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
