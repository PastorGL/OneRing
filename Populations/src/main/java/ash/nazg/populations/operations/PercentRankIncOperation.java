package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.populations.config.ConfigurationParameters.DS_VALUE_COLUMN;

@SuppressWarnings("unused")
public class PercentRankIncOperation extends PopulationIndicatorOperation {
    @Override
    public OperationMeta meta() {
        return new OperationMeta("percentRankInc", "Statistical indicator for 'percentile rank inclusive'" +
                " function for a Double input value column. Output is fixed to value then rank columns. Does not work" +
                " with datasets consisting of less than one element, and returns NaN for single-element dataset",

                new PositionalStreamsMetaBuilder()
                        .ds("Columnar or Pair RDD with value column to calculate the rank",
                                new StreamType[]{StreamType.CSV, StreamType.KeyValue}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_VALUE_COLUMN, "Column for counting rank values, must be of type Double")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("In the case of input Pair RDD rank is calculated for all values under same key," +
                                        " for Columnar for the entire RDD, and output RDD is same type as input",
                                new StreamType[]{StreamType.Fixed, StreamType.KeyValue}
                        )
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        super.configure();

        inputValuesName = opResolver.positionalInput(0);
        inputValuesDelimiter = dsResolver.inputDelimiter(inputValuesName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputValuesName);
        String prop;

        prop = opResolver.definition(DS_VALUE_COLUMN);
        valueColumn = inputColumns.get(prop);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) throws Exception {
        char _inputDelimiter = inputValuesDelimiter;
        int _valueColumn = valueColumn;
        final char _outputDelimiter = outputDelimiter;

        JavaRDDLike inputRdd = input.get(inputValuesName);
        JavaRDDLike output;

        if (inputRdd instanceof JavaRDD) {
            JavaPairRDD<Double, Long> valueCounts = ((JavaRDD<Object>) inputRdd)
                    .mapPartitionsToPair(it -> {
                        CSVParser parser = new CSVParserBuilder()
                                .withSeparator(_inputDelimiter).build();

                        List<Tuple2<Double, Long>> ret = new ArrayList<>();
                        while (it.hasNext()) {
                            Object o = it.next();
                            String l = (o instanceof String) ? (String) o : String.valueOf(o);

                            String[] row = parser.parseLine(l);
                            Double value = Double.parseDouble(row[_valueColumn]);

                            ret.add(new Tuple2<>(value, 1L));
                        }

                        return ret.iterator();
                    })
                    .reduceByKey(Long::sum)
                    .sortByKey();

            final double total = valueCounts.values().reduce(Long::sum) - 1L;

            Map<Integer, Long> partCounts = valueCounts
                    .mapPartitionsWithIndex((idx, it) -> {
                        long ret = 0L;

                        while (it.hasNext()) {
                            ret += it.next()._2;
                        }

                        return Collections.singletonMap(idx, ret).entrySet().iterator();
                    }, true)
                    .collect().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Broadcast<HashMap<Integer, Long>> pc = ctx.broadcast(new HashMap<>(partCounts));
            output = valueCounts
                    .mapPartitionsWithIndex((idx, it) -> {
                        Map<Integer, Long> prevCounts = pc.getValue();

                        double global = prevCounts.entrySet().stream()
                                .filter(e -> e.getKey() < idx)
                                .map(Map.Entry::getValue)
                                .reduce(0L, Long::sum);

                        List<Text> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Double, Long> value = it.next();

                            String[] acc = new String[]{String.valueOf(value._1), String.valueOf(global / total)};
                            for (int j = 0; j < value._2; j++) {
                                StringWriter buffer = new StringWriter();

                                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                                writer.writeNext(acc, false);
                                writer.close();

                                ret.add(new Text(buffer.toString()));
                            }

                            global += value._2;
                        }

                        return ret.iterator();
                    }, true);
        } else {
            output = ((JavaPairRDD<Object, Object>) inputRdd)
                    .mapPartitionsToPair(it -> {
                        CSVParser parser = new CSVParserBuilder()
                                .withSeparator(_inputDelimiter).build();

                        List<Tuple2<Object, Double>> ret = new ArrayList<>();
                        while (it.hasNext()) {
                            Tuple2<Object, Object> t = it.next();

                            Object o = t._2;
                            String l = (o instanceof String) ? (String) o : String.valueOf(o);

                            String[] row = parser.parseLine(l);
                            Double value = Double.parseDouble(row[_valueColumn]);

                            ret.add(new Tuple2<>(t._1, value));
                        }

                        return ret.iterator();
                    })
                    .aggregateByKey(
                            new ArrayList<Double>(),
                            (l, t) -> {
                                l.add(t);
                                Collections.sort(l);
                                return l;
                            },
                            (l1, l2) -> {
                                l1.addAll(l2);
                                Collections.sort(l1);
                                return l1;
                            }
                    )
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Text>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, ArrayList<Double>> next = it.next();

                            ArrayList<Double> value = next._2;

                            int size = value.size();
                            double total = size - 1;
                            int global = 0;
                            for (int j = 0; j < size; j++) {
                                if ((j > 0) && (value.get(j - 1) < value.get(j))) {
                                    global = j;
                                }

                                String[] acc = new String[]{String.valueOf(value.get(j)), String.valueOf(global / total)};

                                StringWriter buffer = new StringWriter();

                                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                                writer.writeNext(acc, false);
                                writer.close();

                                ret.add(new Tuple2<>(next._1, new Text(buffer.toString())));
                            }
                        }

                        return ret.iterator();
                    });
        }

        return Collections.singletonMap(outputName, output);
    }
}
