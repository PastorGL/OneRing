/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math;

import ash.nazg.spark.TestRunner;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class ColumnsMathOperationTest {

    @Test
    @SuppressWarnings("unchecked")
    public void columnsMathTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.columnsMath.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            ret.get("mean").foreach(t -> {
                String[] row = t.toString().split(",");
                double mean = (Double.parseDouble(row[0]) + Double.parseDouble(row[1]) + Double.parseDouble(row[2])) / 3;
                assertEquals(mean, Double.parseDouble(row[3]), 1.E-6);
            });

            ret.get("root_mean").foreach(t -> {
                String[] row = t.toString().split(",");
                final Double first = Double.parseDouble(row[0]);
                final Double second = Double.parseDouble(row[1]);
                final Double third = Double.parseDouble(row[2]);
                double mean = Math.pow(((first * first) + (second * second) + (third * third)) / 3, 0.5);
                assertEquals(mean, Double.parseDouble(row[3]), 1.E-6);
            });

            ret.get("min").foreach(t -> {
                String[] row = t.toString().split(",");
                double min = Doubles.min(Double.parseDouble(row[0]), Double.parseDouble(row[1]), Double.parseDouble(row[2]));
                assertEquals(min, Double.parseDouble(row[3]), 1.E-6);
            });

            ret.get("max").foreach(t -> {
                String[] row = t.toString().split(",");
                double max = Doubles.max(Double.parseDouble(row[0]), Double.parseDouble(row[1]), Double.parseDouble(row[2]));
                assertEquals(max, Double.parseDouble(row[3]), 1.E-6);
            });

            ret.get("median").foreach(t -> {
                String[] row = t.toString().split(",");
                List<Double> dd = Stream.of(Double.parseDouble(row[0]), Double.parseDouble(row[1]), Double.parseDouble(row[2]), Double.parseDouble(row[3]))
                        .sorted().collect(Collectors.toList());

                assertEquals((dd.get(1) + dd.get(2)) / 2, Double.parseDouble(row[4]), 1.E-6);
            });
        }
    }
}