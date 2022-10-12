/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.data.Columnar;
import ash.nazg.data.DataStream;
import ash.nazg.data.Record;
import ash.nazg.data.StreamType;
import ash.nazg.metadata.*;
import ash.nazg.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.*;


@SuppressWarnings("unused")
public class ReachOperation extends Operation {
    public static final String RDD_INPUT_TARGET = "target";
    public static final String RDD_INPUT_SIGNALS = "signals";

    static final String SIGNALS_USERID_ATTR = "signals.userid.attr";
    static final String TARGET_USERID_ATTR = "target.userid.attr";
    static final String TARGET_GROUPING_ATTR = "target.grouping.attr";
    static final String GEN_REACH = "_reach";

    private String signalsUseridAttr;

    private String targetUseridAttr;
    private String targetGroupingAttr;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("reach", "Statistical indicator for some audience reach",

                new NamedStreamsMetaBuilder()
                        .mandatoryInput(RDD_INPUT_SIGNALS, "Source user signals",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}
                        )
                        .mandatoryInput(RDD_INPUT_TARGET, "Target audience signals, a sub-population of base audience signals",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(SIGNALS_USERID_ATTR, "Source DataStream attribute with the user ID")
                        .def(TARGET_USERID_ATTR, "Target audience DataStream attribute with the user ID")
                        .def(TARGET_GROUPING_ATTR, "Target audience DataStream grouping attribute (i.e. grid cell ID)")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Generated DataStream with Reach indicator for each value of grouping attribute, which is in the key",
                                new StreamType[]{StreamType.KeyValue}, Origin.GENERATED, Collections.singletonList(RDD_INPUT_TARGET)
                        )
                        .generated(GEN_REACH, "Reach statistical indicator")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        signalsUseridAttr = params.get(SIGNALS_USERID_ATTR);

        targetUseridAttr = params.get(TARGET_USERID_ATTR);
        targetGroupingAttr = params.get(TARGET_GROUPING_ATTR);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        String _signalsUseridColumn = signalsUseridAttr;

        final long N = ((JavaRDD<Object>) inputStreams.get(RDD_INPUT_SIGNALS).get())
                .mapPartitions(it -> {
                    List<String> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Record row = (Record) it.next();

                        String userid = row.asString(_signalsUseridColumn);

                        ret.add(userid);
                    }

                    return ret.iterator();
                })
                .distinct()
                .count();

        String _targetUseridAttr = targetUseridAttr;
        String _targetGroupingAttr = targetGroupingAttr;

        final List<String> outputColumns = Collections.singletonList(GEN_REACH);

        JavaPairRDD<String, Columnar> output = ((JavaRDD<Object>) inputStreams.get(RDD_INPUT_TARGET).get())
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, String>> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Record row = (Record) it.next();

                        String groupid = row.asString(_targetGroupingAttr);
                        String userid = row.asString(_targetUseridAttr);

                        ret.add(new Tuple2<>(groupid, userid));
                    }

                    return ret.iterator();
                })
                .combineByKey(
                        t1 -> {
                            Set<String> s = new HashSet<>();
                            s.add(t1);
                            return s;
                        },
                        (c, t1) -> {
                            c.add(t1);
                            return c;
                        },
                        (c1, c2) -> {
                            c1.addAll(c2);
                            return c1;
                        }
                )
                .mapToPair(t -> new Tuple2<>(t._1, new Columnar(outputColumns, new Object[]{((double) t._2.size()) / N})));

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.KeyValue, output, Collections.singletonMap("value", outputColumns)));
    }
}
