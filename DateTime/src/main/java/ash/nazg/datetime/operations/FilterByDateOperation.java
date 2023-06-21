/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.operations;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.metadata.OperationMeta;
import ash.nazg.metadata.Origin;
import ash.nazg.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.data.Record;
import ash.nazg.data.DataStream;
import ash.nazg.data.DateTime;
import ash.nazg.data.StreamType;
import ash.nazg.scripting.Operation;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;

@SuppressWarnings("unused")
public class FilterByDateOperation extends Operation {
    public static final String TS_ATTR = "ts.attr";
    public static final String START = "start";
    public static final String END = "end";

    private String dateAttr;

    private Date start;
    private Date end;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("filterByDate", "Filter a Columnar or Spatial DataStream by timestamp attribute" +
                " value between selected dates",

                new PositionalStreamsMetaBuilder()
                        .input("Source Columnar or Spatial DataSteam with timestamp attribute",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Polygon, StreamType.Track}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(TS_ATTR, "Attribute with timestamp in Epoch seconds, milliseconds, or as ISO string")
                        .def(START, "Start of the date range filter (same format)",
                                null, "By default do not filter by range start")
                        .def(END, "End of the date range filter (same format)",
                                null, "By default do not filter by range end")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Filtered Columnar RDD with exploded timestamp attributes",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Polygon, StreamType.Track}, Origin.FILTERED, null
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        dateAttr = params.get(TS_ATTR);

        boolean filteringNeeded = false;

        String prop = params.get(START);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            start = DateTime.parseTimestamp(prop);
        }
        prop = params.get(END);
        if (prop != null && !prop.isEmpty()) {
            filteringNeeded = true;
            end = DateTime.parseTimestamp(prop);
        }

        if (!filteringNeeded) {
            throw new InvalidConfigurationException("Filter by date was not configured for the operation '" + meta.verb + "'");
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final String _col = dateAttr;
        final Date _start = start;
        final Date _end = end;

        DataStream input = inputStreams.getValue(0);
        JavaRDD<Object> output = ((JavaRDD<Object>) input.get())
                .mapPartitions(it -> {
                    List<Object> ret = new ArrayList<>();

                    Calendar cc = Calendar.getInstance();
                    while (it.hasNext()) {
                        Record v = (Record) it.next();

                        boolean matches = true;

                        cc.setTime(DateTime.parseTimestamp(v.asIs(_col)));

                        if (_start != null) {
                            matches = matches && cc.getTime().after(_start);
                        }
                        if (_end != null) {
                            matches = matches && cc.getTime().before(_end);
                        }

                        if (matches) {
                            ret.add(v);
                        }
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(input.streamType, output, input.accessor.attributes()));
    }
}
