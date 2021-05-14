/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Constants;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import javax.xml.bind.DatatypeConverter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public abstract class RDDMetricsPseudoOperation extends Operation {
    private Map<String, String> counterColumns;
    private char outputDelimiter;

    @Override
    public String verb() {
        return null;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(null,
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.DynamicDef("count.column.", String.class)
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(new StreamType[]{
                                StreamType.CSV, StreamType.Fixed, // per column
                                StreamType.KeyValue, // per key
                                StreamType.Point, StreamType.Track, StreamType.Polygon, // per property
                                StreamType.Plain // per record
                        }, true)
                ),

                null
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        counterColumns = new HashMap<>();
        for (String inputName : opResolver.positionalInputs()) {
            String column = opResolver.definition("count.column." + inputName);
            counterColumns.put(inputName, column); // null effectively converts input to Plain
        }

        outputDelimiter = dsResolver.outputDelimiter(Constants.METRICS_DS);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) throws Exception {
        JavaRDD<Text> metricsRdd = (JavaRDD<Text>) input.get(Constants.METRICS_DS);
        if (metricsRdd == null) {
            metricsRdd = ctx.emptyRDD();
        }

        List<Text> metricsList = new ArrayList<>();

        for (String inputName : counterColumns.keySet()) {
            String[] columns = dsResolver.rawInputColumns(inputName);
            final char _inputDelimiter = dsResolver.inputDelimiter(inputName);

            List<String> inputs = getMatchingInputs(input.keySet(), inputName);
            for (String matchingInput : inputs) {
                if (columns == null) {
                    columns = dsResolver.rawInputColumns(matchingInput);
                }
                int idx = -1;
                final String counterColumn = counterColumns.get(inputName);
                if ((counterColumn != null) && (columns != null)) {
                    for (int i = 0; i < columns.length; i++) {
                        if (counterColumn.equals(columns[i])) {
                            idx = i;
                            break;
                        }
                    }
                }
                final int counterIndex = idx;

                JavaRDDLike inputRdd = input.get(matchingInput);
                JavaPairRDD<Object, Object> rdd2 = null;
                if (inputRdd instanceof JavaRDD) {
                    rdd2 = ((JavaRDD<Object>) inputRdd).mapPartitionsToPair(it -> {
                        CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                        MessageDigest md5 = MessageDigest.getInstance("MD5");

                        List<Tuple2<Object, Object>> ret = new ArrayList<>();
                        while (it.hasNext()) {
                            Object o = it.next();
                            Object id;
                            if (o instanceof Geometry) { // Point, Track, Polygon
                                Geometry g = (Geometry) o;

                                id = ((MapWritable) g.getUserData()).get(new Text(counterColumn));
                            } else {
                                String l = (o instanceof String) ? (String) o : String.valueOf(o);

                                if (counterIndex < 0) { // Plain
                                    id = DatatypeConverter.printHexBinary(md5.digest(l.getBytes(StandardCharsets.UTF_8)));
                                } else { // CSV, Fixed
                                    String[] row = parser.parseLine(l);
                                    id = new Text(row[counterIndex]);
                                }
                            }

                            ret.add(new Tuple2<>(id, null));
                        }

                        return ret.iterator();
                    });
                }
                boolean pair = false;
                if (inputRdd instanceof JavaPairRDD) {
                    rdd2 = (JavaPairRDD) inputRdd; // KeyValue
                    pair = true;
                }

                List<Long> counts = rdd2
                        .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum)
                        .values()
                        .sortBy(t -> t, true, 1)
                        .collect();

                int counters = counts.size();
                String values;
                if (counterColumn != null) {
                    if (counterIndex < 0) { // Point, Track, Polygon
                        values = "objects";
                    } else { // CSV, Fixed
                        values = "records";
                    }
                } else {
                    if (pair) { // KeyValue
                        values = "pairs";
                    } else { // Plain
                        values = "opaque records";
                    }
                }
                long total = counts.stream().reduce(Long::sum).orElse(0L);
                double average = (counters == 0) ? 0.D : ((double) total / counters);
                double median = 0.D;
                if (counters != 0) {
                    int m = (counters <= 2) ? 0 : (counters >> 1);
                    median = ((counters % 2) == 0) ? (counts.get(m) + counts.get(m + 1)) / 2.D : counts.get(m).doubleValue();
                }

                StringWriter buffer = new StringWriter();

                CSVWriter writer = new CSVWriter(buffer, outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                writer.writeNext(new String[]{inputName, values, String.valueOf(counterColumn), String.valueOf(total), String.valueOf(counters), String.valueOf(average), String.valueOf(median)}, false);
                writer.close();

                metricsList.add(new Text(buffer.toString()));
            }
        }

        return Collections.singletonMap(Constants.METRICS_DS, ctx.union(metricsRdd, ctx.parallelize(metricsList, 1)));
    }
}
