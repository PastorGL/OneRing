/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.Packages;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.OpInfo;
import ash.nazg.spark.Operations;
import ash.nazg.storage.Adapters;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class PackageService {
    @Inject
    public PackageService() {
    }

    public List<String> getPackages() {
        return new ArrayList<>(Packages.getRegisteredPackages().keySet());
    }

    public List<String> getOperationList(String name) {
        return Operations.getAvailableOperations(name).values().stream()
                .map(OpInfo::verb)
                .collect(Collectors.toList());
    }

    public List<TaskDescriptionLanguage.Operation> getOperations(String name) {
        return Operations.getAvailableOperations(name).values().stream()
                .map(OpInfo::description)
                .collect(Collectors.toList());
    }

    public List<String> getPackageAdapters(String name) {
        return new ArrayList<>(Adapters.getAvailableStorageAdapters(name).keySet());
    }
}
