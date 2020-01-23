package ash.nazg.storage;

import ash.nazg.spark.SparkTask;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Adapters {
    public static final Pattern PATH_PATTERN = Pattern.compile("^[^:]+:/*([^/]+)/(.+)");

    static private final List<InputAdapter> INPUT_ADAPTERS = new ArrayList<>();
    static private final List<OutputAdapter> OUTPUT_ADAPTERS = new ArrayList<>();
    static private InputAdapter fallbackInput = null;
    static private OutputAdapter fallbackOutput = null;

    static {
        try (ScanResult scanResult = new ClassGraph()
                .enableClassInfo()
                .whitelistPackages(SparkTask.getRegisteredPackages().toArray(new String[0]))
                .scan()) {

            ClassInfoList iaClasses = scanResult.getClassesImplementing(InputAdapter.class.getTypeName());
            List<Class<?>> iaClassRefs = iaClasses.loadClasses();

            for (Class<?> iaClass : iaClassRefs) {
                try {
                    InputAdapter ia = (InputAdapter) iaClass.newInstance();
                    if (ia.isFallback()) {
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
                .whitelistPackages(SparkTask.getRegisteredPackages().toArray(new String[0]))
                .scan()) {

            ClassInfoList oaClasses = scanResult.getClassesImplementing(OutputAdapter.class.getTypeName());
            List<Class<?>> oaClassRefs = oaClasses.loadClasses();

            for (Class<?> oaClass : oaClassRefs) {
                try {
                    OutputAdapter oa = (OutputAdapter) oaClass.newInstance();
                    if (oa.isFallback()) {
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

    public static InputAdapter input(String path) {
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
