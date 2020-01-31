/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.tdl.DocumentationGenerator;
import ash.nazg.spark.Operation;
import ash.nazg.spark.SparkTask;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class PackageService {
    @Inject
    public PackageService() {
    }

    public List<String> getPackages() {
        Map<String, Operation.Info> ao = SparkTask.getAvailableOperations();

        List<String> pkgs = new ArrayList<>();

        for (Operation.Info opInfo : ao.values()) {
            String pkgName = opInfo.operationClass.getPackage().getName();
            if (!pkgs.contains(pkgName)) {
                pkgs.add(pkgName);
            }
        }

        return pkgs;
    }

    public List<String> getPackage(String name) {
        Map<String, Operation.Info> ao = SparkTask.getAvailableOperations();

        List<String> ops = new ArrayList<>();

        for (Operation.Info opInfo : ao.values()) {
            String pkgName = opInfo.getClass().getPackage().getName();
            if (pkgName.equals(name)) {
                ops.add(opInfo.verb);
            }
        }

        return ops;
    }

    public String getPackageDoc(String name) {
        Map<String, Operation.Info> ao = SparkTask.getAvailableOperations();

        Map<String, Operation.Info> aop = new HashMap<>();

        for (Operation.Info opInfo : ao.values()) {
            String pkgName = opInfo.getClass().getPackage().getName();
            if (pkgName.equals(name)) {
                aop.put(opInfo.verb, opInfo);
            }
        }

        try (StringWriter writer = new StringWriter()) {
            DocumentationGenerator.packageDoc(aop, writer);
            return writer.toString();
        } catch (Exception ignore) {
        }

        return null;
    }
}
