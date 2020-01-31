/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing.functions;

import com.uber.h3core.H3Core;
import org.apache.hadoop.io.Text;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class H3Function extends HasherFunction {
    public H3Function(int level) {
        super(level);
    }

    @Override
    public Iterator<Tuple2<Text, Text>> call(Iterator<Tuple3<Double, Double, Text>> signals) throws Exception {
        H3Core h3 = H3Core.newInstance();

        List<Tuple2<Text, Text>> ret = new ArrayList<>();
        while (signals.hasNext()) {
            Tuple3<Double, Double, Text> signal = signals.next();

            ret.add(new Tuple2<>(new Text(h3.geoToH3Address(signal._1(), signal._2(), level)), signal._3()));
        }

        return ret.iterator();
    }
}
