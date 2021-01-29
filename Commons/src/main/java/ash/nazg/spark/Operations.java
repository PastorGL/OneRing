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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Operations {
    private static Map<String, OpInfo> availableOperations;

    public static Map<String, OpInfo> getAvailableOperations() {
        if (availableOperations == null) {
            availableOperations = new HashMap<>();

            try (ScanResult scanResult = new ClassGraph()
                    .acceptPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                    .scan()) {

                ClassInfoList operationClasses = scanResult.getSubclasses(Operation.class.getTypeName());
                List<Class<?>> operationClassRefs = operationClasses.loadClasses();

                for (Class<?> opClass : operationClassRefs) {
                    try {
                        if (!Modifier.isAbstract(opClass.getModifiers())) {
                            Operation opInfo = (Operation) opClass.newInstance();
                            availableOperations.put(opInfo.verb(), opInfo);
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Operation class '" + opClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(-8);
                    }
                }

                if (availableOperations.size() == 0) {
                    System.err.println("There are no available Operations in the classpath. Won't continue");
                    System.exit(-8);
                }
            }
        }

        return availableOperations;
    }

    public static Map<String, OpInfo> getAvailableOperations(String pkgName) {
        Map<String, OpInfo> ret = new HashMap<>();

        for (Map.Entry<String, OpInfo> e : getAvailableOperations().entrySet()) {
            if (e.getValue().getClass().getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }
}
