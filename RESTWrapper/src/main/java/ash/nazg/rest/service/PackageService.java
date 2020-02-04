/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.Packages;
import ash.nazg.config.tdl.DocumentationGenerator;
import ash.nazg.spark.Operation;
import ash.nazg.spark.Operations;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class PackageService {
    @Inject
    public PackageService() {
    }

    public List<String> getPackages() {
        return new ArrayList<>(Packages.getRegisteredPackages().keySet());
    }

    public List<String> getPackage(String name) {
        Map<String, Operation.Info> ao = Operations.getAvailableOperations(name);

        List<String> ops = new ArrayList<>();

        for (Operation.Info opInfo : ao.values()) {
            ops.add(opInfo.verb);
        }

        return ops;
    }

    public String getPackageDoc(String name) {
        try (StringWriter writer = new StringWriter()) {
            DocumentationGenerator.packageDoc(name, writer);
            return writer.toString();
        } catch (Exception ignore) {
        }

        return null;
    }
}
