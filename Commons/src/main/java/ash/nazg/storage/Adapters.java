/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.Packages;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Adapters {
    public static final Pattern PATH_PATTERN = Pattern.compile("^([^:]+:/*[^/]+)/(.+)");

    static private final List<InputAdapter> INPUT_ADAPTERS = new ArrayList<>();
    static private final List<OutputAdapter> OUTPUT_ADAPTERS = new ArrayList<>();
    static private InputAdapter fallbackInput = null;
    static private OutputAdapter fallbackOutput = null;

    static {
        try (ScanResult scanResult = new ClassGraph()
                .enableClassInfo()
                .acceptPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                .scan()) {

            ClassInfoList iaClasses = scanResult.getClassesImplementing(InputAdapter.class.getTypeName());
            List<Class<?>> iaClassRefs = iaClasses.loadClasses();

            for (Class<?> iaClass : iaClassRefs) {
                try {
                    InputAdapter ia = (InputAdapter) iaClass.newInstance();
                    if (ia instanceof HadoopAdapter) {
                        fallbackInput = ia;
                    } else {
                        INPUT_ADAPTERS.add(ia);
                    }
                } catch (Exception e) {
                    System.err.println("Cannot instantiate Input Adapter class '" + iaClass.getTypeName() + "'");
                    e.printStackTrace(System.err);
                    System.exit(-8);
                }
            }
        }

        if (fallbackInput != null) {
            INPUT_ADAPTERS.add(fallbackInput);
        } else {
            System.err.println("There is no fallback Input Adapter in the classpath. Won't continue");
            System.exit(-8);
        }

        try (ScanResult scanResult = new ClassGraph()
                .enableClassInfo()
                .acceptPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                .scan()) {

            ClassInfoList oaClasses = scanResult.getClassesImplementing(OutputAdapter.class.getTypeName());
            List<Class<?>> oaClassRefs = oaClasses.loadClasses();

            for (Class<?> oaClass : oaClassRefs) {
                try {
                    OutputAdapter oa = (OutputAdapter) oaClass.newInstance();
                    if (oa instanceof HadoopAdapter) {
                        fallbackOutput = oa;
                    } else {
                        OUTPUT_ADAPTERS.add(oa);
                    }
                } catch (Exception e) {
                    System.err.println("Cannot instantiate Output Adapter class '" + oaClass.getTypeName() + "'");
                    e.printStackTrace(System.err);
                    System.exit(-8);
                }
            }
        }

        if (fallbackOutput != null) {
            OUTPUT_ADAPTERS.add(fallbackOutput);
        } else {
            System.err.println("There is no fallback Output Adapter in the classpath. Won't continue");
            System.exit(-8);
        }
    }

    static public Map<String, StorageAdapter> getAvailableInputAdapters(String pkgName) {
        Map<String, StorageAdapter> ret = new HashMap<>();

        INPUT_ADAPTERS.forEach(e -> {
            if (e.getClass().getPackage().getName().equals(pkgName)) {
                ret.put(e.getClass().getSimpleName(), e);
            }
        });

        return ret;
    }

    static public Map<String, StorageAdapter> getAvailableOutputAdapters(String pkgName) {
        Map<String, StorageAdapter> ret = new HashMap<>();

        OUTPUT_ADAPTERS.forEach(e -> {
            if (e.getClass().getPackage().getName().equals(pkgName)) {
                ret.put(e.getClass().getSimpleName(), e);
            }
        });

        return ret;
    }

    static public InputAdapter getInputAdapter(String name) {
        return INPUT_ADAPTERS.stream().filter(e -> e.getClass().getSimpleName().equals(name)).findFirst().orElse(null);
    }

    static public OutputAdapter getOutputAdapter(String name) {
        return OUTPUT_ADAPTERS.stream().filter(e -> e.getClass().getSimpleName().equals(name)).findFirst().orElse(null);
    }

    static public InputAdapter input(String path) {
        for (InputAdapter ia : INPUT_ADAPTERS) {
            if (ia.proto().matcher(path).matches()) {
                return ia;
            }
        }

        return fallbackInput;
    }

    static public OutputAdapter output(String path) {
        for (OutputAdapter oa : OUTPUT_ADAPTERS) {
            if (oa.proto().matcher(path).matches()) {
                return oa;
            }
        }

        return fallbackOutput;
    }
}
