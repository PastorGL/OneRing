/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math;

import ash.nazg.spark.TestRunner;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ColumnsMathOperationTest {

    @Test
    @SuppressWarnings("unchecked")
    public void columnsMathTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.columnsMath.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            ret.get("sum").foreach(t -> {
                String[] row = t.toString().split(",");
                assertEquals(new Double(row[0]), new Double(row[1]) + 15, 1.E-6);
            });

            ret.get("star").foreach(t -> {
                String[] row = t.toString().split(",");
                assertEquals(new Double(row[0]), new Double(row[1]), 1.E-6);
            });

            ret.get("mean").foreach(t -> {
                String[] row = t.toString().split(",");
                double mean = (new Double(row[0]) + new Double(row[1]) + new Double(row[2])) / 3;
                assertEquals(mean, new Double(row[3]), 1.E-6);
            });

            ret.get("root_mean").foreach(t -> {
                String[] row = t.toString().split(",");
                final Double first = new Double(row[0]);
                final Double second = new Double(row[1]);
                final Double third = new Double(row[2]);
                double mean = Math.pow(((first * first) + (second * second) + (third * third)) / 3, 0.5);
                assertEquals(mean, new Double(row[3]), 1.E-6);
            });

            ret.get("min").foreach(t -> {
                String[] row = t.toString().split(",");
                double min = Doubles.min(new Double(row[0]), new Double(row[1]), new Double(row[2]));
                assertEquals(min, new Double(row[3]), 1.E-6);
            });

            ret.get("max").foreach(t -> {
                String[] row = t.toString().split(",");
                double max = Doubles.max(new Double(row[0]), new Double(row[1]), new Double(row[2]));
                assertEquals(max, new Double(row[3]), 1.E-6);
            });

            ret.get("mul").foreach(t -> {
                String[] row = t.toString().split(",");
                double mul = 3.5 * new Double(row[0]) * new Double(row[1]) * new Double(row[2]);
                assertEquals(mul, new Double(row[3]), 1.E-6);
            });
        }
    }
}