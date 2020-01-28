package ash.nazg.config;

import ash.nazg.config.tdl.DocumentationGenerator;
import ash.nazg.config.tdl.PropertiesConverter;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spark.SparkTask;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

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

            Map<String, Operation.Info> ao = SparkTask.getAvailableOperations();
            final Map<String, Map<String, Operation.Info>> aop = new HashMap<>();
            ObjectMapper om = new ObjectMapper();
            om.configure(SerializationFeature.INDENT_OUTPUT, true);
            DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
            pp.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
            final ObjectWriter ow = om.writer(pp);
            ao.forEach((verb, opInfo) -> {
                try (FileWriter writer = new FileWriter(outputDirectory + "/operation/" + verb + ".md")) {
                    DocumentationGenerator.operationDoc(opInfo, writer);
                } catch (Exception ignore) {
                }

                String pkgName = opInfo.operationClass.getPackage().getName();
                aop.compute(pkgName, (k, v) -> {
                    Map<String, Operation.Info> aopp = (v == null)
                            ? new HashMap<>()
                            : v;
                    aopp.put(opInfo.verb, opInfo);

                    return aopp;
                });

                try {
                    TaskDefinitionLanguage.Task exampleTask = DocumentationGenerator.createExampleTask(opInfo, null);
                    String exampleDir = outputDirectory + "/operation/" + verb;
                    Files.createDirectories(Paths.get(exampleDir));
                    try (FileWriter writer = new FileWriter(new File(exampleDir, "example.json"))) {
                        ow.writeValue(writer, exampleTask);
                    }
                    try (final Writer writer = new BufferedWriter(new FileWriter(new File(exampleDir, "example.ini")))) {
                        Properties props = PropertiesConverter.toProperties(exampleTask);

                        new TreeMap<>(props).forEach((k, v) -> {
                            try {
                                writer.write(k + "=" + v + "\n");
                            } catch (IOException ignore) {
                            }
                        });
                    }
                } catch (Exception ignore) {
                }
            });

            aop.forEach((pkgName, aopp) -> {
                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".md")) {
                    DocumentationGenerator.packageDoc(aopp, writer);
                } catch (Exception ignore) {
                }
            });

            try (FileWriter writer = new FileWriter(outputDirectory + "/index.md")) {
                DocumentationGenerator.indexDoc(ao, writer);
            } catch (Exception ignore) {
            }
        } catch (Exception e) {
            System.out.println("Error while generating descriptions:");
            e.printStackTrace();

            System.exit(-7);
        }
    }
}
