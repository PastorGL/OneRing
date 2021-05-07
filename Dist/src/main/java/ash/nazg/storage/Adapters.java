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
import java.util.List;

public class Adapters {
    static private final List<AdapterInfo> INPUT_ADAPTERS = new ArrayList<>();
    static private final List<AdapterInfo> OUTPUT_ADAPTERS = new ArrayList<>();

    static {
        try (ScanResult scanResult = new ClassGraph()
                .enableClassInfo()
                .acceptPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                .scan()) {

            ClassInfoList iaClasses = scanResult.getSubclasses(InputAdapter.class.getTypeName());
            List<Class<?>> iaClassRefs = iaClasses.loadClasses();

            for (Class<?> iaClass : iaClassRefs) {
                try {
                    InputAdapter ia = (InputAdapter) iaClass.newInstance();
                    AdapterInfo ai = new AdapterInfo(ia.proto(), (Class<? extends StorageAdapter>) iaClass);
                    INPUT_ADAPTERS.add(ai);
                } catch (Exception e) {
                    System.err.println("Cannot instantiate Input Adapter class '" + iaClass.getTypeName() + "'");
                    e.printStackTrace(System.err);
                    System.exit(-8);
                }
            }
        }

        try (ScanResult scanResult = new ClassGraph()
                .enableClassInfo()
                .acceptPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                .scan()) {

            ClassInfoList oaClasses = scanResult.getSubclasses(OutputAdapter.class.getTypeName());
            List<Class<?>> oaClassRefs = oaClasses.loadClasses();

            for (Class<?> oaClass : oaClassRefs) {
                try {
                    OutputAdapter oa = (OutputAdapter) oaClass.newInstance();
                    AdapterInfo ai = new AdapterInfo(oa.proto(), (Class<? extends StorageAdapter>) oaClass);
                    OUTPUT_ADAPTERS.add(ai);
                } catch (Exception e) {
                    System.err.println("Cannot instantiate Output Adapter class '" + oaClass.getTypeName() + "'");
                    e.printStackTrace(System.err);
                    System.exit(-8);
                }
            }
        }
    }

    static public Class<? extends InputAdapter> input(String path) {
        for (AdapterInfo ia : INPUT_ADAPTERS) {
            if (ia.proto.matcher(path).matches()) {
                return (Class<? extends InputAdapter>) ia.adapterClass;
            }
        }

        return null;
    }

    static public Class<? extends OutputAdapter> output(String path) {
        for (AdapterInfo oa : OUTPUT_ADAPTERS) {
            if (oa.proto.matcher(path).matches()) {
                return (Class<? extends OutputAdapter>) oa.adapterClass;
            }
        }

        return null;
    }
}
