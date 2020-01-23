package ash.nazg.commons.functions;

import org.apache.hadoop.io.Text;
import org.apache.spark.HashPartitioner;
import org.apache.spark.util.Utils;
import scala.Tuple2;

public class TrackPartitioner extends HashPartitioner {
    public TrackPartitioner(int partitions) {
        super(partitions);
    }

    @Override
    public int getPartition(Object key) {
        if (key == null) {
            return 0;
        }

        Tuple2<Text, Double> k = (Tuple2<Text, Double>) key;
        return Utils.nonNegativeMod(k._1.hashCode(), numPartitions());
    }
}
