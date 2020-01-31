/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.populations.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
@Deprecated
public class PopulationFilterOperation extends Operation {
    public static final String VERB = "populationFilter";

    private String inputName;
    private char inputDelimiter;
    private String outputName;
    private char outputDelimiter;

    private Integer yearColumn;
    private Integer monthColumn;
    private Integer dayColumn;
    private Integer useridColumn;
    private Integer silosColumn;

    private List<Tuple3<Integer, Byte, Byte>> dates;
    private Integer minDays;
    private Integer minSignals;
    private Integer maxSignals;

    @Override
    @Description("Heuristic that classifies and splits the user tracks into a defined set of track types")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_POP_FILTER_DATES, String[].class),
                        new TaskDescriptionLanguage.Definition(OP_POP_FILTER_MIN_DAYS, Integer.class),
                        new TaskDescriptionLanguage.Definition(OP_POP_FILTER_MIN_SIGNALS, Integer.class),
                        new TaskDescriptionLanguage.Definition(OP_POP_FILTER_MAX_SIGNALS, Integer.class),
                        new TaskDescriptionLanguage.Definition(DS_SIGNALS_YEAR_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_SIGNALS_MONTH_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_SIGNALS_DAY_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_SIGNALS_USERID_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_SIGNALS_SILOS_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_INPUT_SIGNALS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                        }
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_OUTPUT_SIGNALS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        false
                                ),
                        }
                )
        );
    }

    @Override
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);

        inputName = describedProps.namedInputs.get(RDD_INPUT_SIGNALS);
        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);
        outputName = describedProps.namedOutputs.get(RDD_OUTPUT_SIGNALS);
        outputDelimiter = dataStreamsProps.outputDelimiter(outputName);

        String[] filterDates = describedProps.defs.getTyped(OP_POP_FILTER_DATES);
        dates = Arrays.stream(filterDates).distinct().map(d -> {
            String[] date = d.split("-");
            Integer year = new Integer(date[0]);
            Byte month = new Byte(date[1]);
            Byte day = new Byte(date[2]);
            return new Tuple3<>(year, month, day);
        }).collect(Collectors.toList());

        minDays = describedProps.defs.getTyped(OP_POP_FILTER_MIN_DAYS);
        if (minDays < 0) {
            throw new InvalidConfigValueException("Value of '" + OP_POP_FILTER_MIN_DAYS + "' definition in the task '" + name + "' must be > 0");
        }

        minSignals = describedProps.defs.getTyped(OP_POP_FILTER_MIN_SIGNALS);
        if (minSignals < 0) {
            throw new InvalidConfigValueException("Value of '" + OP_POP_FILTER_MIN_SIGNALS + "' definition in the task '" + name + "' must be > 0");
        }

        maxSignals = describedProps.defs.getTyped(OP_POP_FILTER_MAX_SIGNALS);
        if (maxSignals < minSignals) {
            throw new InvalidConfigValueException("Value of '" + OP_POP_FILTER_MIN_SIGNALS + "' definition in the task '" + name + "' must be less than '" + OP_POP_FILTER_MAX_SIGNALS + "' for the same task");
        }

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);
        String prop;

        prop = describedProps.defs.getTyped(DS_SIGNALS_YEAR_COLUMN);
        yearColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(DS_SIGNALS_MONTH_COLUMN);
        monthColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(DS_SIGNALS_DAY_COLUMN);
        dayColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(DS_SIGNALS_USERID_COLUMN);
        useridColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(DS_SIGNALS_SILOS_COLUMN);
        silosColumn = inputColumns.get(prop);
    }

    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final String _outputDelimiter = "" + outputDelimiter;

        final Integer _yearColumn = yearColumn;
        final Integer _monthColumn = monthColumn;
        final Integer _dayColumn = dayColumn;
        final Integer _useridColumn = useridColumn;
        final Integer _silosColumn = silosColumn;

        final List<Tuple3<Integer, Byte, Byte>> _dates = dates;
        final Integer _minDays = minDays;
        final Integer _minSignals = minSignals;
        final Integer _maxSignals = maxSignals;

        //userId, <date, silos, source text>
        JavaPairRDD<Text, Tuple2<Tuple3<IntWritable, ByteWritable, ByteWritable>, IntWritable>> signals =
                ((JavaRDD<Object>) input.get(inputName))
                        .mapToPair(o -> {
                            String l = o instanceof String ? (String) o : String.valueOf(o);
                            CSVParser parser = new CSVParserBuilder()
                                    .withSeparator(_inputDelimiter).build();
                            String[] row = parser.parseLine(l);

                            Text userId = new Text(row[_useridColumn]);
                            IntWritable silos = new IntWritable(new Integer(row[_silosColumn]));

                            IntWritable year = new IntWritable(new Integer(row[_yearColumn]));
                            ByteWritable month = new ByteWritable(new Byte(row[_monthColumn]));
                            ByteWritable day = new ByteWritable(new Byte(row[_dayColumn]));

                            Tuple3<IntWritable, ByteWritable, ByteWritable> date =
                                    new Tuple3<>(year, month, day);

                            return new Tuple2<>(userId, new Tuple2<>(date, silos));
                        });

        JavaPairRDD<Text, Tuple2<Tuple3<IntWritable, ByteWritable, ByteWritable>, IntWritable>> filteredByDay = signals
                .filter(t -> {
                    Tuple3<IntWritable, ByteWritable, ByteWritable> writableDay = t._2._1;
                    int year = writableDay._1().get();
                    byte month = writableDay._2().get();
                    byte day = writableDay._3().get();
                    for (Tuple3<Integer, Byte, Byte> d : _dates) {
                        if (year == d._1() && month == d._2() && day == d._3()) {
                            return true;
                        }
                    }
                    return false;
                });

        // userid, <silos, signals count, days count>
        JavaRDD<Text> output = filteredByDay
                .combineByKey(
                        // init combiner
                        t -> {
                            // use set to collect unique days
                            Set<Tuple3<IntWritable, ByteWritable, ByteWritable>> days = new HashSet<>();
                            days.add(t._1());
                            // take first one silos. Unique for user
                            Integer silos = t._2().get();
                            return new Tuple3<>(silos, 1, days);
                        },
                        // put values into combiner
                        (c, t) -> {
                            Set<Tuple3<IntWritable, ByteWritable, ByteWritable>> days = c._3();
                            days.add(t._1);
                            return new Tuple3<>(c._1(), c._2() + 1, days);
                        },
                        // combine combiner, well.
                        (c1, c2) -> {
                            Set<Tuple3<IntWritable, ByteWritable, ByteWritable>> days = c1._3();
                            days.addAll(c2._3());
                            Integer count = c1._2() + c2._2();
                            Integer silos = c1._1();
                            return new Tuple3<>(silos, count, days);
                        })
                // transform to <silos, signals count, days count>
                .mapValues(t -> new Tuple3<>(t._1(), t._2(), t._3().size()))
                .filter(t -> {
                    Tuple3<Integer, Integer, Integer> countValues = t._2;
                    Integer signalsCount = countValues._2();
                    Integer days = countValues._3();

                    return signalsCount >= _minSignals
                            && signalsCount <= _maxSignals
                            && days >= _minDays;
                })
                .map(t -> {
                    StringJoiner sj = new StringJoiner(_outputDelimiter);
                    sj.add(t._1.toString())
                            .add(t._2._1().toString())
                            .add(t._2._2().toString())
                            .add(t._2._3().toString());
                    return new Text(sj.toString());
                });

        return Collections.singletonMap(outputName, output);
    }
}
