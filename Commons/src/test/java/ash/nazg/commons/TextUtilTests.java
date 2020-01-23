package ash.nazg.commons;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TextUtilTests {
    private Text IN = new Text("I will find anything\tinside a string\tthat is     looong\tan' beautiful");
    private Text IN2 = new Text("\tI will find anything\tinside a string\tthat is     looong\tan' beautiful\t");
    private Text IN3 = new Text("");

    @Test
    public void testFind() {
        assertEquals("I will find anything", TextUtil.column(IN, 0).toString());
        assertEquals("that is     looong", TextUtil.column(IN, 2).toString());

        assertEquals("inside a string", TextUtil.columns(IN, 1, 1).toString());
        assertEquals("that is     looong\tan' beautiful", TextUtil.columns(IN, 2, 2).toString());
        assertEquals("that is     looong\tan' beautiful", TextUtil.columns(IN, 2).toString());
        assertNull(TextUtil.column(IN, 5));

        assertEquals("", TextUtil.column(IN2, 0).toString());
        assertEquals("that is     looong", TextUtil.column(IN2, 3).toString());
        assertEquals("", TextUtil.column(IN2, 5).toString());

        assertEquals("that is     looong\tan' beautiful", TextUtil.columns(IN2, 3, 2).toString());
        assertEquals("that is     looong\tan' beautiful\t", TextUtil.columns(IN2, 3).toString());
        assertEquals("that is     looong\tan' beautiful\t", TextUtil.columns(IN2, 3, 10).toString());
    }

    @Test
    public void testCount() {
        assertEquals(4, TextUtil.columnCount(IN));
        assertEquals(6, TextUtil.columnCount(IN2));
        assertEquals(1, TextUtil.columnCount(IN3));
    }

    @Test
    public void testAppend() {
        assertEquals("two", TextUtil.append(null, new Text("two")).toString());
        assertEquals("one\ttwo", TextUtil.append(new Text("one"), new Text("two")).toString());
        assertEquals("one;two", TextUtil.append(new Text("one"), new Text(";"), new Text("two")).toString());
        assertEquals("two\t1.945", TextUtil.append(new Text("two"), 1.945d).toString());
    }
}
