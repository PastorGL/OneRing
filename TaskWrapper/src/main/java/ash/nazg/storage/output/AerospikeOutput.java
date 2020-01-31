/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.output;

import ash.nazg.config.DataStreamsConfig;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.storage.AerospikeAdapter;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.google.common.collect.Iterators;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.OutputAdapter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class AerospikeOutput extends AerospikeAdapter implements OutputAdapter {
    private static final Pattern PATTERN = Pattern.compile("^aero:([^/]+)/(.+)");

    private String[] cols;

    @Override
    public Pattern proto() {
        return PATTERN;
    }

    @Override
    public void setProperties(String outputName, WrapperConfig wrapperConfig) throws InvalidConfigValueException {
        aerospikeHost = wrapperConfig.getOutputProperty("aerospike.host", "localhost");
        aerospikePort = Integer.valueOf(wrapperConfig.getOutputProperty("aerospike.port", "3000"));

        DataStreamsConfig adapterConfig = new DataStreamsConfig(wrapperConfig.getProperties(), null, null, Collections.singleton(outputName), Collections.singleton(outputName), null);

        cols = adapterConfig.outputColumns.get(outputName);
        delimiter = adapterConfig.outputDelimiter(outputName);
    }

    @Override
    public void save(String path, JavaRDDLike rdd) {
        if (rdd instanceof JavaPairRDD) {
            Matcher m = proto().matcher(path);
            m.matches();

            final String _ns = m.group(1);
            final String _set = m.group(2);
            final char _delimiter = delimiter;
            final String[] _cols = cols;
            final String _aerospikeHost = aerospikeHost;
            final Integer _aerospikePort = aerospikePort;

            ((JavaPairRDD<Object, Object>) rdd).mapPartitions((FlatMapFunction<Iterator<Tuple2<Object, Object>>, Object>) partition -> {
                AerospikeClient _client = new AerospikeClient(_aerospikeHost, _aerospikePort);

                CSVParser parser = new CSVParserBuilder().withSeparator(_delimiter).build();

                while (partition.hasNext()) {
                    Tuple2<Object, Object> v = partition.next();

                    Key key = new Key(_ns, _set, String.valueOf(v._1));

                    String[] row = parser.parseLine(String.valueOf(v._2));
                    List<Bin> bins = new ArrayList<>();
                    for (int i = 0; i < _cols.length; i++) {
                        if (!_cols[i].equals("_")) {
                            bins.add(new Bin(_cols[i], row[i]));
                        }
                    }
                    _client.put(null, key, bins.toArray(new Bin[0]));
                }

                return Iterators.emptyIterator();
            }).count();
        }
    }
}
