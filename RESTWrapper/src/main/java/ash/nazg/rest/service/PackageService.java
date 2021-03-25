/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.Packages;
import ash.nazg.config.tdl.DocumentationGenerator;
import ash.nazg.config.tdl.TaskDocumentationLanguage;
import ash.nazg.spark.Operations;
import ash.nazg.storage.Adapters;
import ash.nazg.storage.StorageAdapter;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.print.Doc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class PackageService {
    @Inject
    public PackageService() {
    }

    public List<TaskDocumentationLanguage.Package> getPackages() throws Exception {
        List<TaskDocumentationLanguage.Package> ret = new ArrayList<>();

        for (Map.Entry<String, String> p : Packages.getRegisteredPackages().entrySet()) {
            ret.add(DocumentationGenerator.packageDoc(p.getKey(), p.getValue()));
        }

        return ret;
    }

    public TaskDocumentationLanguage.Operation getOperation(String name) throws Exception {
        return DocumentationGenerator.operationDoc(Operations.getAvailableOperations().get(name));
    }

    public TaskDocumentationLanguage.Adapter getInputAdapter(String name) throws Exception {
        return DocumentationGenerator.adapterDoc(Adapters.getInputAdapter(name));
    }

    public TaskDocumentationLanguage.Adapter getOutputAdapter(String name) throws Exception {
        return DocumentationGenerator.adapterDoc(Adapters.getOutputAdapter(name));
    }
}
