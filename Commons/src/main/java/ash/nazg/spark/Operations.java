/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.Packages;
import ash.nazg.config.TaskConfig;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.apache.spark.api.java.JavaSparkContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.*;

public class Operations {
    private static Map<String, Operation.Info> availableOperations;

    protected TaskConfig taskConfig;
    protected JavaSparkContext context;
    protected List<Operation> opChain = new LinkedList<>();

    public Operations(JavaSparkContext context) {
        this.context = context;
    }

    public static Map<String, Operation.Info> getAvailableOperations() {
        if (availableOperations == null) {
            availableOperations = new HashMap<>();

            try (ScanResult scanResult = new ClassGraph()
                    .enableClassInfo()
                    .whitelistPackages(Packages.getRegisteredPackages().keySet().toArray(new String[0]))
                    .scan()) {

                ClassInfoList operationClasses = scanResult.getSubclasses(Operation.class.getTypeName());
                List<Class<?>> operationClassRefs = operationClasses.loadClasses();

                for (Class<?> opClass : operationClassRefs) {
                    try {
                        if (!Modifier.isAbstract(opClass.getModifiers())) {
                            Map.Entry<String, Operation.Info> opInfo = ((Operation) opClass.newInstance()).info();
                            availableOperations.put(opInfo.getKey(), opInfo.getValue());
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

    public static Map<String, Operation.Info> getAvailableOperations(String pkgName) {
        Map<String, Operation.Info> ret = new HashMap<>();

        for (Map.Entry<String, Operation.Info> e : getAvailableOperations().entrySet()) {
            if (e.getValue().operationClass.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }

    public List<Operation> instantiateOperations() throws InvalidConfigValueException {
        if (opChain.isEmpty()) {
            Map<String, Operation.Info> ao = getAvailableOperations();

            for (Map.Entry<String, String> operation : taskConfig.getOperations().entrySet()) {
                String verb = operation.getValue();
                String name = operation.getKey();

                if (!ao.containsKey(verb)) {
                    throw new InvalidConfigValueException("Operation '" + name + "' has unknown verb '" + verb + "'");
                }

                Operation chainedOp;
                try {
                    Operation.Info opInfo = ao.get(verb);

                    Constructor<? extends OperationConfig> configClass = opInfo.configClass.getConstructor(Properties.class, TaskDescriptionLanguage.Operation.class, String.class);
                    OperationConfig config = configClass.newInstance(taskConfig.getProperties(), opInfo.description, name);

                    chainedOp = opInfo.operationClass.newInstance();
                    chainedOp.setName(name);
                    chainedOp.setConfig(config);
                } catch (Exception e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof InvalidConfigValueException) {
                        throw (InvalidConfigValueException) cause;
                    }

                    throw new InvalidConfigValueException("Cannot instantiate operation '" + verb + "' named '" + name + "' with an exception", e);
                }
                opChain.add(chainedOp);
            }
        }

        if (opChain.isEmpty()) {
            throw new InvalidConfigValueException("Operation chain not configured for the task");
        }

        opChain.forEach(operation -> operation.setContext(context));

        return opChain;
    }

    public void setTaskConfig(TaskConfig taskConfig) {
        this.taskConfig = taskConfig;
    }
}