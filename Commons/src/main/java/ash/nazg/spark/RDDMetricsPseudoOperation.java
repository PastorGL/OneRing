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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RDDMetricsPseudoOperation extends Operation {
    private Map<String, String> counterColumns;

    private Map<String, Map<String, Double>> metrics;

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
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) throws Exception {
        metrics = new HashMap<>();

        for (String inputName : counterColumns.keySet()) {
            String[] columns = dsResolver.rawInputColumns(inputName);
            Character delim = dsResolver.inputDelimiter(inputName);

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

                if (delim == null) {
                    delim = dsResolver.inputDelimiter(matchingInput);
                }
                if (delim == null) {
                    delim = dsResolver.inputDelimiter(Constants.DEFAULT_DS);
                }
                final char _inputDelimiter = delim;

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

                int size = counts.size();
                Map<String, Double> map = new HashMap<>();
                String counter, values;
                if (counterColumn != null) {
                    if (counterIndex < 0) { // Point, Track, Polygon
                        counter = "Count of unique objects by property '" + counterColumn + "'";
                        values = "objects";
                    } else { // CSV, Fixed
                        counter = "Count of unique records by column '" + counterColumn + "'";
                        values = "records";
                    }
                } else {
                    if (pair) { // KeyValue
                        counter = "Count of unique pair keys";
                        values = "pairs";
                    } else { // Plain
                        counter = "Count of unique opaque records";
                        values = "opaque records";
                    }
                }
                map.put(counter, (double) size);
                double num = counts.stream().reduce(Long::sum).orElse(0L).doubleValue();
                map.put("Total number of " + values, num);
                map.put("Average number of " + values + " per counter", (size == 0) ? 0.D : (num / size));
                String median = "Median number of " + values + " per counter";
                if (size == 0) {
                    map.put(median, 0.D);
                } else {
                    int m = size >> 1;
                    if (size % 2 == 0) {
                        map.put(median, (counts.get(m) + counts.get(m + 1)) / 2.D);
                    } else {
                        map.put(median, counts.get(m).doubleValue());
                    }
                }

                metrics.put(matchingInput, map);
            }
        }

        return null;
    }

    public Map<String, Map<String, Double>> getMetrics() {
        return metrics;
    }
}
