/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import ash.nazg.config.tdl.*;
import ash.nazg.spark.OpInfo;
import ash.nazg.spark.Operations;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class DocGen {
    public static void main(String[] args) {
        try {
            final String outputDirectory = args[0];

            if (new File(outputDirectory).exists()) {
                Files.walk(Paths.get(outputDirectory))
                        .map(Path::toFile)
                        .sorted((o1, o2) -> -o1.compareTo(o2))
                        .forEach(File::delete);

            }
            Files.createDirectories(Paths.get(outputDirectory, "package"));
            Files.createDirectories(Paths.get(outputDirectory, "operation"));

            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADERS, "classpath");
            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER + ".classpath.class", ClasspathResourceLoader.class.getCanonicalName());
            Velocity.init();

            Map<String, String> pkgs = Packages.getRegisteredPackages();

            for (Map.Entry<String, String> pkg : pkgs.entrySet()) {
                String pkgName = pkg.getKey();

                ObjectMapper om = new TDLObjectMapper();
                om.configure(SerializationFeature.INDENT_OUTPUT, true);
                DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
                pp.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
                final ObjectWriter ow = om.writer(pp);

                for (Map.Entry<String, OpInfo> entry : Operations.getAvailableOperations(pkgName).entrySet()) {
                    String verb = entry.getKey();
                    OpInfo opInfo = entry.getValue();
                    try (FileWriter mdWriter = new FileWriter(outputDirectory + "/operation/" + verb + ".md")) {
                        TaskDocumentationLanguage.Operation opDoc = DocumentationGenerator.operationDoc(opInfo);

                        VelocityContext vc = new VelocityContext();
                        vc.put("op", opDoc);

                        Template operation = Velocity.getTemplate("operation.vm", StandardCharsets.UTF_8.name());
                        operation.merge(vc, mdWriter);

                        TaskDefinitionLanguage.Task exampleTask = DocumentationGenerator.createExampleTask(opInfo, opDoc, null);
                        String exampleDir = outputDirectory + "/operation/" + verb;
                        Files.createDirectories(Paths.get(exampleDir));
                        try (FileWriter jsonWriter = new FileWriter(new File(exampleDir, "example.json"))) {
                            ow.writeValue(jsonWriter, exampleTask);
                        }
                        try (final Writer iniWriter = new BufferedWriter(new FileWriter(new File(exampleDir, "example.ini")))) {
                            PropertiesWriter.writeProperties(exampleTask, iniWriter);
                        }
                    } catch (Exception e) {
                        throw new Exception("Operation '" + verb + "'", e);
                    }
                }

                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".md")) {
                    DocumentationGenerator.packageDoc(pkgName, writer);
                } catch (Exception e) {
                    throw new Exception("Package '" + pkgName + "'", e);
                }
            }

            try (FileWriter writer = new FileWriter(outputDirectory + "/index.md")) {
                DocumentationGenerator.indexDoc(pkgs, writer);
            } catch (Exception e) {
                throw new Exception("Index", e);
            }
        } catch (Exception e) {
            System.out.println("Error while generating documentation:");
            e.printStackTrace();

            System.exit(-7);
        }
    }
}
