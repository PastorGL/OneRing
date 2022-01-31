/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.commons.functions.*;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ash.nazg.config.tdl.StreamType.*;

@SuppressWarnings("unused")
public class ColumnarToolboxOperation extends Operation {
    public static final String OP_QUERY = "query";

    private String inputName;
    private char inputDelimiter;
    private String[] rawColumns;
    private Map<String, Integer> inputCols;
    private boolean union;
    private UnionSpec unionSpec;
    private boolean join;
    private JoinSpec joinSpec;
    private boolean star;

    private String outputName;
    private char outputDelimiter;
    private List<List<Expressions.ExprItem<?>>> outputCols;

    private List<Expressions.ExprItem<?>> query;
    private Long limitRecords;
    private Double limitPercent;

    private List<List<Expressions.ExprItem<?>>> subQueries;
    private String[] subInputs;
    private char[] subDelimiters;
    private String[][] subColumns;
    private int[] subItems;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("columnarToolbox", "This operation allows SELECT queries against Columnar RDDs using any of their columns" +
                " as criteria, e.g. \"SELECT name, type, color FROM boxes WHERE (name LIKE 'A.*' OR type = 'near') AND width > 25\"." +
                " This also supports \"[INNER|LEFT|RIGHT|OUTER] JOIN pair1,pair2,...\" to join Pair RDDs by their keys," +
                " and \"UNION [CAT|AND|XOR] (ds1,ds2,...|ds*)\" to union any RDDs in the FROM clause." +
                " If join is performed, column references in both SELECT and WHERE must be fully-qualified." +
                " For UNIONs to succeed, they must be of same type and have same columns",

                new PositionalStreamsMetaBuilder()
                        .ds("1st Columnar RDD is for main query, any subsequent for subqueries in the order of incidence",
                                new StreamType[]{CSV, Fixed, KeyValue}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_QUERY, "Query for record columns, SQL SELECT-like")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Same type of input, with records adhering to a query",
                                new StreamType[]{Passthru}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputName = opResolver.positionalOutput(0);
        outputDelimiter = dsResolver.outputDelimiter(outputName);

        rawColumns = dsResolver.rawInputColumns(inputName);
        inputCols = new HashMap<>();
        for (int i = 0; i < rawColumns.length; i++) {
            inputCols.put(rawColumns[i], i);
        }

        String queryString = opResolver.definition(OP_QUERY);
        CharStream cs = CharStreams.fromString(queryString);

        QueryLexicon lexer = new QueryLexicon(cs);
        QueryParser parser = new QueryParser(new CommonTokenStream(lexer));
        QueryListenerImpl listener = new QueryListenerImpl();
        parser.addParseListener(listener);
        parser.parse();

        List<String> fromSet = listener.getFromSet();
        if (listener.getUnion() != null) {
            union = true;
            unionSpec = listener.getUnion();
            if (listener.isStarFrom()) {
                inputName = fromSet.get(0) + "*";
            } else {
                inputName = String.join(",", fromSet);
            }
        }
        if (listener.getJoinSpec() != null) {
            join = true;
            joinSpec = listener.getJoinSpec();
            inputName = String.join(",", fromSet);
        }

        query = listener.getQuery();

        if (listener.isStarItems()) {
            outputCols = new ArrayList<>();
            for (String colName : rawColumns) {
                outputCols.add(Collections.singletonList(Expressions.propItem(colName)));
            }
            star = true;
        } else {
            outputCols = listener.getItems();

            for (List<Expressions.ExprItem<?>> exprItem : outputCols) {
                if (exprItem.get(0) instanceof Expressions.SpatialItem) {
                    throw new InvalidConfigValueException("Operation '" + name + "' doesn't supports spatial type queries");
                }
            }
        }

        limitRecords = listener.getLimitRecords();
        limitPercent = listener.getLimitPercent();

        subQueries = listener.getSubQueries();
        if (!subQueries.isEmpty()) {
            int subSize = subQueries.size();
            subInputs = new String[subSize];
            subDelimiters = new char[subSize];
            subColumns = new String[subSize][];
            subItems = new int[subSize];

            for (int i = 0; i < subSize; i++) {
                String subName = opResolver.positionalInput(i + 1);
                subInputs[i] = subName;
                subDelimiters[i] = dsResolver.inputDelimiter(subName);
                subColumns[i] = dsResolver.rawInputColumns(subName);

                Map<String, Integer> subColumns = dsResolver.inputColumns(subName);
                String subItem = listener.getSubItems().get(i).get(0).toString();
                if (!subItem.startsWith(subName + ".")) {
                    subItem = subName + "." + subItem;
                }
                subItems[i] = subColumns.get(subItem);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final List<Expressions.ExprItem<?>> _query = query;
        final char _inputDelimiter = inputDelimiter;
        final char _outputDelimiter = outputDelimiter;

        JavaRDDLike srcRdd;

        if (union) {
            List<String> inputs = getMatchingInputs(input.keySet(), inputName);
            boolean pair = false;

            List<JavaPairRDD<Object, Integer>> paired = new ArrayList<>();
            final int inpNumber = inputs.size();
            for (int i = 0; i < inpNumber; i++) {
                JavaRDDLike inp = input.get(inputs.get(i));
                if (inp instanceof JavaPairRDD) {
                    if (i == 0) {
                        pair = true;
                    } else if (!pair) {
                        throw new InvalidConfigValueException("Can't UNION Pair and non-Pair RDDs in operation '" + name + "'");
                    }
                } else if (pair) {
                    throw new InvalidConfigValueException("Can't UNION Pair and non-Pair RDDs in operation '" + name + "'");
                }

                final Integer ii = i;
                paired.add(inp.mapToPair(v -> new Tuple2<>(v, ii)));
            }

            JavaPairRDD<Object, Integer> union = ctx.<Object, Integer>union(paired.toArray(new JavaPairRDD[0]));
            JavaRDD<Object> u;
            switch (unionSpec) {
                case XOR: {
                    u = union
                            .groupByKey()
                            .mapValues(it -> {
                                final Set<Integer> inpSet = new HashSet<>();
                                final long[] count = new long[1];
                                it.forEach(i -> {
                                    inpSet.add(i);
                                    count[0]++;
                                });

                                if (inpSet.size() > 1) {
                                    return 0L;
                                } else {
                                    return count[0];
                                }
                            })
                            .flatMap(t -> Stream.generate(() -> t._1).limit(t._2).iterator());
                    break;
                }
                case AND: {
                    u = union
                            .groupByKey()
                            .mapValues(it -> {
                                Iterator<Integer> iter = it.iterator();
                                Set<Integer> inpSet = new HashSet<>();
                                Map<Integer, Long> counts = new HashMap<>();
                                while (iter.hasNext()) {
                                    Integer ii = iter.next();
                                    inpSet.add(ii);
                                    counts.compute(ii, (i, v) -> {
                                        if (v == null) {
                                            return 1L;
                                        }
                                        return v + 1L;
                                    });
                                }
                                if (inpSet.size() < inpNumber) {
                                    return 0L;
                                } else {
                                    return counts.values().stream().mapToLong(Long::longValue).reduce(Math::min).getAsLong();
                                }
                            })
                            .flatMap(t -> Stream.generate(() -> t._1).limit(t._2).iterator());
                    break;
                }
                default: {
                    u = union
                            .keys();
                    break;
                }
            }

            srcRdd = pair ? u.mapToPair(t -> (Tuple2) t) : u;
        } else if (join) {
            List<String> inputs = getMatchingInputs(input.keySet(), inputName);

            final int inpNumber = inputs.size();
            inputCols = new HashMap<>();
            for (String jInput : inputs) {
                JavaRDDLike inp = input.get(jInput);
                if (!(inp instanceof JavaPairRDD)) {
                    throw new InvalidConfigValueException("Can't JOIN non-Pair RDD in operation '" + name + "'");
                }

                int s = inputCols.size();
                dsResolver.inputColumns(jInput).forEach((n, i) -> inputCols.put(n, i + s));
            }

            int accSize = inputCols.size();
            rawColumns = new String[accSize];
            int j = 0;
            if (star) {
                outputCols = new ArrayList<>();
            }
            for (String jInput : inputs) {
                Map<Integer, String> revInputCols = dsResolver.inputColumns(jInput).entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
                for (int i = 0; i < revInputCols.size(); i++) {
                    rawColumns[j + i] = revInputCols.get(i);
                }

                if (star) {
                    for (String colName : dsResolver.rawInputColumns(jInput)) {
                        outputCols.add(Collections.singletonList(Expressions.propItem(jInput + "." + colName)));
                    }
                }

                j += revInputCols.size();
            }

            JavaPairRDD<Object, Object[]> leftInputRDD = ((JavaPairRDD<Object, Object>) input.get(inputs.get(0))).mapPartitionsToPair(it -> {
                List<Tuple2<Object, Object[]>> res = new ArrayList<>();

                CSVParser parse0 = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                final String[] accTemplate = new String[accSize];

                while (it.hasNext()) {
                    Tuple2<Object, Object> o = it.next();

                    String l = String.valueOf(o._2);
                    String[] line = parse0.parseLine(l);

                    String[] acc = accTemplate.clone();
                    System.arraycopy(line, 0, acc, 0, line.length);
                    res.add(new Tuple2<>(o._1, acc));
                }

                return res.iterator();
            });

            int pos = dsResolver.rawInputColumns(inputs.get(0)).length;
            for (int r = 1; r < inputs.size(); r++) {
                String jInput = inputs.get(r);
                JavaPairRDD<Object, Object> rightInputRDD = (JavaPairRDD<Object, Object>) input.get(jInput);

                JavaPairRDD partialJoin;
                switch (joinSpec) {
                    case LEFT:
                        partialJoin = leftInputRDD.leftOuterJoin(rightInputRDD);
                        break;
                    case RIGHT:
                        partialJoin = leftInputRDD.rightOuterJoin(rightInputRDD);
                        break;
                    case OUTER:
                        partialJoin = leftInputRDD.fullOuterJoin(rightInputRDD);
                        break;
                    default:
                        partialJoin = leftInputRDD.join(rightInputRDD);
                }

                char inpDel = dsResolver.inputDelimiter(jInput);
                int rightSize = dsResolver.rawInputColumns(jInput).length;
                int _pos = pos;
                leftInputRDD = partialJoin.mapPartitionsToPair(ito -> {
                    List<Tuple2> res = new ArrayList<>();

                    Iterator<Tuple2> it = (Iterator) ito;
                    CSVParser parseRight = new CSVParserBuilder().withSeparator(inpDel).build();
                    final String[] accTemplate = new String[accSize];

                    while (it.hasNext()) {
                        Tuple2<Object, Object> o = it.next();

                        Tuple2<Object, Object> v = (Tuple2<Object, Object>) o._2;

                        String[] left = null;
                        if (v._1 instanceof org.apache.spark.api.java.Optional) {
                            org.apache.spark.api.java.Optional o1 = (org.apache.spark.api.java.Optional) v._1;
                            if (o1.isPresent()) {
                                left = (String[]) o1.get();
                            }
                        } else {
                            left = (String[]) v._1;
                        }

                        String line = null;
                        if (v._2 instanceof org.apache.spark.api.java.Optional) {
                            org.apache.spark.api.java.Optional o2 = (Optional) v._2;
                            if (o2.isPresent()) {
                                line = String.valueOf(o2.get());
                            }
                        } else {
                            line = String.valueOf(v._2);
                        }
                        String[] right = (line != null) ? parseRight.parseLine(line) : new String[rightSize];

                        String[] acc = (left != null) ? left.clone() : accTemplate.clone();

                        System.arraycopy(right, 0, acc, _pos, rightSize);

                        res.add(new Tuple2<>(o._1, acc));
                    }

                    return res.iterator();
                });
                pos += rightSize;
            }

            srcRdd = leftInputRDD;
        } else {
            srcRdd = input.get(inputName);
        }

        final String[] _rawColumns = rawColumns;
        Map<String, Integer> _inputColumns = inputCols;
        final List<List<Expressions.ExprItem<?>>> _what = outputCols;

        ArrayList<Set<Object>> subSets = null;
        if (!subQueries.isEmpty()) {
            int subSize = subQueries.size();
            subSets = new ArrayList<>();

            for (int i = 0; i < subSize; i++) {
                final char _subDelimiter = subDelimiters[i];
                final List<Expressions.ExprItem<?>> _subQuery = subQueries.get(i);
                final int _subItem = subItems[i];
                final String[] _subColumns = subColumns[i];

                JavaRDDLike subRDD = input.get(subInputs[i]);
                JavaRDD<Object> objectRDD;
                if (subRDD instanceof JavaPairRDD) {
                    objectRDD = ((JavaPairRDD) subRDD).values();
                } else {
                    objectRDD = (JavaRDD) subRDD;
                }

                List<Object> matchSet = objectRDD
                        .mapPartitions(it -> {
                            CSVParser parser = new CSVParserBuilder().withSeparator(_subDelimiter).build();

                            BiFunction<Object, String, Object> propGetter = (Object props, String prop) -> ((Map) props).get(prop);

                            Set<Object> ret = new HashSet<>();
                            while (it.hasNext()) {
                                Object v = it.next();
                                String l = v instanceof String ? (String) v : String.valueOf(v);

                                String[] line = parser.parseLine(l);

                                Map props = new HashMap();
                                for (int j = 0; j < line.length; j++) {
                                    props.put(_subColumns[j], line[j]);
                                }

                                if (Operator.bool(propGetter, props, null, _subQuery)) {
                                    ret.add(line[_subItem]);
                                }
                            }

                            return ret.iterator();
                        })
                        .distinct()
                        .collect();

                subSets.add(new HashSet<>(matchSet));
            }
        }
        Broadcast<ArrayList<Set<Object>>> subBroadcast = ctx.broadcast(subSets);

        JavaRDDLike output;

        if (srcRdd instanceof JavaRDD) {
            output = ((JavaRDD<Object>) srcRdd)
                    .mapPartitions(it -> {
                        List<Text> ret = new ArrayList<>();

                        BiFunction<Object, String, Object> propGetter = (Object props, String prop) -> ((Map) props).get(prop);
                        ArrayList<Set<Object>> subs = subBroadcast.getValue();

                        CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                        while (it.hasNext()) {
                            Object v = it.next();
                            String l = v instanceof String ? (String) v : String.valueOf(v);

                            String[] line = parser.parseLine(l);

                            Map props = new HashMap();
                            for (int i = 0; i < line.length; i++) {
                                props.put(_rawColumns[i], line[i]);
                            }

                            if (Operator.bool(propGetter, props, subs, _query)) {
                                StringWriter buffer = new StringWriter();
                                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                                String[] columns = new String[_what.size()];
                                int i = 0;
                                for (List<Expressions.ExprItem<?>> wi : _what) {
                                    columns[i] = String.valueOf(Operator.eval(propGetter, props, null, wi));

                                    i++;
                                }

                                writer.writeNext(columns, false);
                                writer.close();

                                ret.add(new Text(buffer.toString()));
                            }
                        }

                        return ret.iterator();
                    });

            if (limitRecords != null) {
                output = ((JavaRDD) output).sample(false, limitRecords.doubleValue() / output.count());
            }
            if (limitPercent != null) {
                output = ((JavaRDD) output).sample(false, limitPercent);
            }
        } else {
            output = ((JavaPairRDD<Object, Object>) srcRdd)
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Object>> ret = new ArrayList<>();

                        BiFunction<Object, String, Object> propGetter = (Object props, String prop) -> ((Map) props).get(prop);
                        ArrayList<Set<Object>> subs = subBroadcast.getValue();

                        CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                        while (it.hasNext()) {
                            Tuple2<Object, Object> v = it.next();

                            String[] line;
                            if (v._2 instanceof Object[]) {
                                line = (String[]) v._2;
                            } else {
                                String l = v._2 instanceof String ? (String) v._2 : String.valueOf(v._2);

                                line = parser.parseLine(l);
                            }

                            Map props = new HashMap();
                            for (int i = 0; i < line.length; i++) {
                                props.put(_rawColumns[i], line[i]);
                            }

                            if (Operator.bool(propGetter, props, subs, _query)) {
                                StringWriter buffer = new StringWriter();
                                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                                String[] columns = new String[_what.size()];
                                int i = 0;
                                for (List<Expressions.ExprItem<?>> wi : _what) {
                                    columns[i] = String.valueOf(Operator.eval(propGetter, props, null, wi));

                                    i++;
                                }

                                writer.writeNext(columns, false);
                                writer.close();

                                ret.add(new Tuple2<>(v._1, new Text(buffer.toString())));
                            }
                        }

                        return ret.iterator();
                    });

            if (limitRecords != null) {
                output = ((JavaPairRDD) output).sample(false, limitRecords.doubleValue() / output.count());
            }
            if (limitPercent != null) {
                output = ((JavaPairRDD) output).sample(false, limitPercent);
            }
        }

        return Collections.singletonMap(outputName, output);
    }
}
