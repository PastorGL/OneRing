/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.Descriptions;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.OpInfo;
import ash.nazg.spark.Operation;
import ash.nazg.spark.Operations;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.config.OperationConfig.COLUMN_SUFFIX;

public class Guardian {
    public static void main(String[] args) {
        System.out.println("This is a Guardian of the metadata in the One Ring Spark Modules.\n" +
                "If it fails, check the classes mentioned in the output, and fix the errors.\n" +
                " ... stands for the name of registered packages " + String.join(", ", Packages.getRegisteredPackages().keySet()));

        Set<Class<? extends Enum<?>>> interestingEnums = new HashSet<>();

        Map<String, OpInfo> ao = Operations.getAvailableOperations();

        final List<String> errors = new ArrayList<>();

        for (Map.Entry<String, OpInfo> oi : ao.entrySet()) {
            Class<? extends OpInfo> opClass = oi.getValue().getClass();

            final String cnAbbr = mangleCN(opClass);

            try {
                Method verb = opClass.getDeclaredMethod("verb");

                boolean described = false;
                try {
                    Description d = verb.getDeclaredAnnotation(Description.class);
                    described = !d.value().isEmpty();
                } catch (NullPointerException ignore) {
                }
                if (!described) {
                    errors.add(cnAbbr + " method verb() does not have a proper TDL @Description");
                }
            } catch (NoSuchMethodException e) {
                errors.add(cnAbbr + " does not have method named verb()");
            }

            Descriptions ds = Descriptions.inspectOperation(opClass);

            boolean described = false;
            for (Field field : ds.fields.values()) {
                try {
                    Description d = field.getDeclaredAnnotation(Description.class);
                    described = !d.value().isEmpty();
                } catch (NullPointerException ignore) {
                }
                if (!described) {
                    errors.add(mangleCN(field.getDeclaringClass()) + " field " + field.getName() + " does not have a proper TDL @Description");
                }
            }

            TaskDescriptionLanguage.Operation descr = oi.getValue().description();

            List<String> columnBasedInputs = new ArrayList<>();

            if (descr.inputs.named != null) {
                Arrays.stream(descr.inputs.named)
                        .forEach(ns -> {
                            if (ns.columnBased) {
                                columnBasedInputs.add(ns.name);
                            }

                            if (!ds.inputs.containsKey(ns.name)) {
                                errors.add(cnAbbr + " has a named input '" + ns.name + "' without a proper TDL @Description");
                            }
                        });
            }

            if (descr.definitions != null) {
                for (TaskDescriptionLanguage.DefBase db : descr.definitions) {
                    if (db instanceof TaskDescriptionLanguage.Definition) {
                        TaskDescriptionLanguage.Definition def = (TaskDescriptionLanguage.Definition) db;

                        if (!ds.definitions.containsKey(def.name)) {
                            errors.add(cnAbbr + " has a definition '" + def.name + "' without a proper TDL @Description");
                        }

                        if (!columnBasedInputs.isEmpty() && def.name.endsWith(COLUMN_SUFFIX)) {
                            String rddName = def.name.split("\\.", 2)[0];

                            if (!columnBasedInputs.contains(rddName)) {
                                errors.add(cnAbbr + " has a named input column definition '" + def.name + "' that does not correspond to any named inputs");
                            }
                        }

                        if (def.clazz.isEnum()) {
                            interestingEnums.add(def.clazz);
                        }

                        if (def.optional) {
                            if (def.clazz.isEnum()) {
                                try {
                                    Enum.valueOf(def.clazz, def.defaults);
                                } catch (Exception ignored) {
                                    errors.add(cnAbbr + " has an optional definition '" + def.name + "' of enum type " + mangleCN(def.clazz) + " with invalid default value '" + def.defaults + "'");
                                }
                            }

                            String strippedName = ds.definitions.get(def.name)
                                    .replaceFirst("^DS_", "")
                                    .replaceFirst("^OP_", "");

                            if (!ds.defaults.containsKey("DEF_" + strippedName)) {
                                errors.add(cnAbbr + " has an optional definition '" + def.name + "' without a properly described default value '" + def.defaults + "'");
                            }
                        }
                    } else {
                        TaskDescriptionLanguage.DynamicDef dyn = (TaskDescriptionLanguage.DynamicDef) db;

                        if (!ds.definitions.containsKey(dyn.prefix)) {
                            errors.add(cnAbbr + " has dynamic definitions without a properly described prefix '" + dyn.prefix + "'");
                        }
                    }
                }
            }

            if (descr.outputs.positional != null) {
                if (descr.outputs.positional.generatedColumns != null) {
                    Arrays.stream(descr.outputs.positional.generatedColumns)
                            .forEach(gen -> {
                                if (!ds.generated.containsKey(gen)) {
                                    errors.add(cnAbbr + " positional output has a generated column '" + gen + "' without a proper TDL @Description");
                                }
                            });
                }
            } else if (descr.outputs.named != null) {
                Arrays.stream(descr.outputs.named)
                        .forEach(ns -> {
                            if (!ds.outputs.containsKey(ns.name)) {
                                errors.add(cnAbbr + " has a named output '" + ns.name + "' without a proper TDL @Description");
                            }
                            if (ns.generatedColumns != null) {
                                Arrays.stream(ns.generatedColumns)
                                        .forEach(gen -> {
                                            if (!ds.generated.containsKey(gen)) {
                                                errors.add(cnAbbr + " named output '" + ns.name + "' has a generated column '" + gen + "' without a proper TDL @Description");
                                            }
                                        });
                            }
                        });
            }

        }

        for (Class<? extends Enum<?>> en : interestingEnums) {
            Arrays.stream(en.getEnumConstants())
                    .forEach((Enum<?> e) -> {
                        boolean described = false;
                        try {
                            Description d = e.getClass().getField(e.name()).getDeclaredAnnotation(Description.class);
                            described = !d.value().isEmpty();
                        } catch (Exception ignore) {
                        }
                        if (!described) {
                            errors.add(mangleCN(e.getClass()) + " constant " + e.name() + " does not have a proper TDL @Description");
                        }
                    });
        }

        List<String> distinctErrors = errors.stream().distinct().collect(Collectors.toList());
        if (!distinctErrors.isEmpty()) {
            distinctErrors.forEach(System.err::println);

            System.err.println("Congratulations! You have " + distinctErrors.size() + " ERROR(S). See the full list above this line");
            System.exit(-10);
        } else {
            System.out.println("Passed");
        }
    }

    private static String registeredPackageClassName(Class<?> clazz, String prefix) {
        String pkgName = clazz.getPackage().getName();

        Optional<String> foundPackage = Packages.getRegisteredPackages().keySet().stream()
                .filter(pkgName::startsWith)
                .findFirst();

        return foundPackage.map(s -> clazz.getName().replace(s + ".", prefix)).orElse(clazz.getName());
    }

    private static String mangleCN(Class<?> clazz) {
        return clazz.isEnum()
                ? registeredPackageClassName(clazz, "Enum ...")
                : registeredPackageClassName(clazz, "Operation ...")
                ;
    }
}
