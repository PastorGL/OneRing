/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import ash.nazg.config.tdl.Description;
import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.PackageInfo;
import io.github.classgraph.ScanResult;

import java.util.HashMap;
import java.util.Map;

public class Packages {
    private static final Map<String, String> registeredPackages = new HashMap<String, String>() {{
        try (ScanResult scanResult = new ClassGraph()
                .enableAnnotationInfo()
                .scan()) {

            for (PackageInfo pi : scanResult.getPackageInfo()) {
                AnnotationInfo ai = pi.getAnnotationInfo(Description.class.getCanonicalName());
                if (ai != null) {
                    put(pi.getName(), ai.getParameterValues().getValue("value").toString());
                }
            }
        }
    }};

    public static Map<String, String> getRegisteredPackages() {
        return registeredPackages;
    }
}
