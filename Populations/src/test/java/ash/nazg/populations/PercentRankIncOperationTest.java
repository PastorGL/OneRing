package ash.nazg.populations;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PercentRankIncOperationTest {
    @Test
    public void simpleRdd() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/test1.percentRankInc.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> dataset = (JavaRDD<Text>) ret.get("result");

            List<Double> resMap = dataset.map(t -> {
                String[] s = String.valueOf(t).split("\t", 2);
                return Double.parseDouble(s[1]);
            }).collect();

            assertEquals(0.D, resMap.get(0), 1E-06);
            assertEquals(0.875D, resMap.get(8), 1E-06);
        }
    }

    @Test
    public void pairRdd() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/test2.percentRankInc.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaPairRDD<Text, Text> dataset = (JavaPairRDD<Text, Text>) ret.get("result");

            Map<Text, ArrayList<Double>> resMap = dataset.combineByKey(t -> {
                        ArrayList<Double> r = new ArrayList<>();
                        String[] s = String.valueOf(t).split("\t", 2);
                        r.add(Double.parseDouble(s[1]));
                        return r;
                    },
                    (l, t) -> {
                        String[] s = String.valueOf(t).split("\t", 2);
                        l.add(Double.parseDouble(s[1]));
                        return l;
                    },
                    (l1, l2) -> {
                        l1.addAll(l2);
                        return l1;
                    }
            ).collectAsMap();

            assertEquals(0.571D, resMap.get(new Text("a")).get(4), 1E-03);
            assertEquals(0.428D, resMap.get(new Text("b")).get(4), 1E-03);
            assertEquals(0.428D, resMap.get(new Text("c")).get(4), 1E-03);
            assertEquals(0.571D, resMap.get(new Text("d")).get(4), 1E-03);
            assertEquals(0.D, resMap.get(new Text("e")).get(4), 1E-03);
            assertEquals(0.D, resMap.get(new Text("f")).get(4), 1E-03);
        }
    }
}
