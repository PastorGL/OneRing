/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.Packages;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Operations {
    public final static Map<String, OpInfo> availableOperations;

    static {
        Map<String, OpInfo> operations = new HashMap<>();

        try (ScanResult scanResult = new ClassGraph()
                .acceptPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                .scan()) {

            ClassInfoList operationClasses = scanResult.getSubclasses(Operation.class.getTypeName());
            List<Class<?>> operationClassRefs = operationClasses.loadClasses();

            for (Class<?> opClass : operationClassRefs) {
                try {
                    if (!Modifier.isAbstract(opClass.getModifiers())) {
                        Operation op = (Operation) opClass.newInstance();
                        String verb = op.verb();
                        operations.put(verb, new OpInfo(verb, (Class<? extends Operation>) opClass, op.description));
                    }
                } catch (Exception e) {
                    System.err.println("Cannot instantiate Operation class '" + opClass.getTypeName() + "'");
                    e.printStackTrace(System.err);
                    System.exit(-8);
                }
            }

            if (operations.size() == 0) {
                System.err.println("There are no available Operations in the classpath. Won't continue");
                System.exit(-8);
            }
        }

        availableOperations = Collections.unmodifiableMap(operations);
    }

    public static Map<String, OpInfo> getAvailableOperations(String pkgName) {
        Map<String, OpInfo> ret = new HashMap<>();

        for (Map.Entry<String, OpInfo> e : availableOperations.entrySet()) {
            if (e.getValue().opClass.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }

    public static RDDMetricsPseudoOperation getMetricsOperation() {
        return new RDDMetricsPseudoOperation() {
        };
    }
}
