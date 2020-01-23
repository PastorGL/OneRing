package ash.nazg.commons.functions;

import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TrackComparator implements Comparator<Tuple2<Text, Double>>, Serializable {
    @Override
    public int compare(Tuple2<Text, Double> o1, Tuple2<Text, Double> o2) {
        return Double.compare(o1._2, o2._2);
    }
}
