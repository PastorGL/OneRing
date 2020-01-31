/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import ash.nazg.spark.Operation;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Descriptions {
    static {
        Velocity.setProperty(RuntimeConstants.RESOURCE_LOADERS, "classpath");
        Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER + ".classpath.class", ClasspathResourceLoader.class.getCanonicalName());
        Velocity.init();
    }

    public final Package opPackage;
    public final Map<String, Field> fields;
    public final Map<String, String> definitions;
    public final Map<String, Object> defaults;
    public final Map<String, String> inputs;
    public final Map<String, String> outputs;
    public final Map<String, String> generated;

    private Descriptions(Class<? extends Operation> opClass) {
        opPackage = opClass.getPackage();

        Map<String, Field> interesting = getInterestingFields(opClass);
        try {
            Class cpClass = Class.forName(opPackage.getName().replace(".operations", ".config.ConfigurationParameters"));
            interesting.putAll(getInterestingFields(cpClass));
        } catch (ClassNotFoundException ignore) {
        }

        fields = Collections.unmodifiableMap(interesting);

        definitions = Collections.unmodifiableMap(interesting.entrySet().stream()
                .filter(f -> f.getKey().startsWith("OP_") || f.getKey().startsWith("DS_"))
                .collect(Collectors.toMap(f -> String.valueOf(Descriptions.getField(f.getValue())), Map.Entry::getKey))
        );

        defaults = Collections.unmodifiableMap(interesting.entrySet().stream()
                .filter(f -> f.getKey().startsWith("DEF_"))
                // work around https://bugs.openjdk.java.net/browse/JDK-8148463
                .collect(HashMap::new, (m, f) -> {
                    Object value = getField(f.getValue());
                    String key = f.getKey();
                    if (value instanceof String[]) {
                        m.put(key, String.join(",", (String[]) value));
                    } else {
                        m.put(key, String.valueOf(value));
                    }
                }, HashMap::putAll)
        );

        inputs = Collections.unmodifiableMap(interesting.entrySet().stream()
                .filter(f -> f.getKey().startsWith("RDD_INPUT_"))
                .collect(Collectors.toMap(f -> String.valueOf(Descriptions.getField(f.getValue())), Map.Entry::getKey))
        );

        outputs = Collections.unmodifiableMap(interesting.entrySet().stream()
                .filter(f -> f.getKey().startsWith("RDD_OUTPUT_"))
                .collect(Collectors.toMap(f -> String.valueOf(Descriptions.getField(f.getValue())), Map.Entry::getKey))
        );

        generated = Collections.unmodifiableMap(interesting.entrySet().stream()
                .filter(f -> f.getKey().startsWith("GEN_"))
                .collect(Collectors.toMap(f -> String.valueOf(Descriptions.getField(f.getValue())), Map.Entry::getKey))
        );
    }

    private static Object getField(Field f) {
        try {
            return f.get(null);
        } catch (Exception ignore) {
            return -0xBAD_F00D;
        }
    }

    public static Descriptions inspectOperation(Class<? extends Operation> opClass) {
        return new Descriptions(opClass);
    }

    private Map<String, Field> getInterestingFields(Class<?> interestedClass) {
        Map<String, Field> ret = new HashMap<>();

        Class<?> superclass = interestedClass.getSuperclass();
        if ((superclass != Operation.class) && (superclass != Object.class)) {
            ret.putAll(getInterestingFields(superclass));
        }

        for (Field field : interestedClass.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers)) {
                String name = field.getName();
                if (name.startsWith("DS_") || name.startsWith("OP_")
                        || name.startsWith("DEF_")
                        || name.startsWith("GEN_")
                        || name.startsWith("RDD_")) {
                    ret.put(name, field);
                }
            }
        }

        return ret;
    }
}
