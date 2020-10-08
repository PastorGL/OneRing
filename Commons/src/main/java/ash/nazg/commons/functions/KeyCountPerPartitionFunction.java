package ash.nazg.commons.functions;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.*;

public class KeyCountPerPartitionFunction<V> implements Function<JavaPairRDD<Tuple2<Text, Double>, V>, Map<Integer, Integer>> {
    @Override
    public HashMap<Integer, Integer> call(JavaPairRDD<Tuple2<Text, Double>, V> toCount) {
        // collect counts of userids per each partition to a map
        Map<Integer, Integer> map = toCount
                .mapPartitionsWithIndex((idx, it) -> {
                    List<Tuple2<Integer, Integer>> num = new ArrayList<>();

                    Set<Text> userids = new HashSet<>();
                    while (it.hasNext()) {
                        Text userid = it.next()._1._1;
                        userids.add(userid);
                    }

                    num.add(new Tuple2<>(idx, userids.size()));

                    return num.iterator();
                }, true) // preserve partitions!
                .mapToPair(t -> t)
                .collectAsMap();

        return new HashMap<>(map);
    }
}
