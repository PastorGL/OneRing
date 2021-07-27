/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.functions;

import ash.nazg.commons.functions.TrackComparator;
import ash.nazg.commons.functions.TrackPartitioner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MedianCalcFunction implements Function<JavaPairRDD<Text, Double>, JavaRDD<Tuple2<Text, Double>>> {
    private JavaSparkContext ctx;

    public MedianCalcFunction(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    public JavaRDD<Tuple2<Text, Double>> call(JavaPairRDD<Text, Double> gidToScores) {
        final int _partCount = gidToScores.getNumPartitions();
        JavaPairRDD<Text, Double> polygonRawScore = gidToScores
                .mapToPair(t -> new Tuple2<>(t, (Void) null))
                .repartitionAndSortWithinPartitions(new TrackPartitioner(_partCount), new TrackComparator())
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Text, Double>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Tuple2<Text, Double>, Void> t = it.next();

                        ret.add(t._1);
                    }

                    return ret.iterator();
                }, true);

        Broadcast<HashMap<Text, Long>> numScoresPerPolygon = ctx.broadcast(new HashMap<>(polygonRawScore
                .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum)
                .collectAsMap())
        );

        return polygonRawScore
                .mapPartitionsWithIndex((idx, it) -> {
                    final Map<Text, Long> _numScoresPerPolygon = numScoresPerPolygon.getValue();

                    Map<Text, Tuple3<Long, Double, Double>> medians = new HashMap<>();

                    while (it.hasNext()) {
                        Tuple2<Text, Double> groupScore = it.next();

                        Text groupid = groupScore._1;
                        long medianIndex = _numScoresPerPolygon.get(groupid) >> 1;

                        Tuple3<Long, Double, Double> t3 = medians.compute(groupid, (text, t) ->
                                (t == null) ? new Tuple3<>(0L, null, null) : t
                        );

                        long currentIndex = t3._1();
                        if (currentIndex < medianIndex - 1) {
                            medians.put(groupid, new Tuple3<>(currentIndex + 1, null, null));
                        } else if (currentIndex == medianIndex - 1) {
                            medians.put(groupid, new Tuple3<>(currentIndex + 1, groupScore._2, null));
                        } else if (currentIndex == medianIndex) {
                            medians.put(groupid, new Tuple3<>(currentIndex + 1, t3._2(), groupScore._2));
                        }
                    }

                    List<Tuple2<Text, Double>> ret = medians.entrySet().stream()
                            .map(e -> {
                                Text groupid = e.getKey();
                                Tuple3<Long, Double, Double> t3 = e.getValue();

                                Double median;
                                if (_numScoresPerPolygon.get(groupid) % 2 == 0) {
                                    median = (t3._2() + t3._3()) / 2.D;
                                } else {
                                    median = t3._3();
                                }

                                return new Tuple2<>(groupid, median);
                            })
                            .collect(Collectors.toList());

                    return ret.iterator();
                }, true);
    }
}
