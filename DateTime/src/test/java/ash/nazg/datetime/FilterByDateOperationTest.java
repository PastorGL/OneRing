/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime;

import ash.nazg.spark.TestRunner;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class FilterByDateOperationTest {
    @Test
    public void filterByDateTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.filterByDate.properties")) {

            List<String> result = underTest.go().get("filtered").collect();

            assertTrue(0 < result.size());
            assertTrue(underTest.go().get("ts_data").count() > result.size());

            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

            List<String> months = Arrays.asList("2", "4", "6", "8", "12");

            for (String t : result) {
                String[] ll = parser.parseLine(t);

                assertNotEquals("2017", ll[9]);
                assertFalse(months.contains(ll[8]));
            }


        }
    }

    @Test
    public void filterByTimeOfDayTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.filterByDate.properties")) {

            List<String> result = underTest.go().get("tod").collect();

            assertTrue(0 < result.size());
            assertTrue(underTest.go().get("ts_data").count() > result.size());

            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

            List<String> hours = Arrays.asList("8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19");

            for (String t : result) {
                String[] ll = parser.parseLine(t);

                assertFalse(hours.contains(ll[10]));
            }


        }
    }
}
