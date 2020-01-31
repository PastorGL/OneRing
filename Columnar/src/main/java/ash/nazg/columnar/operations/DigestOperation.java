/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.columnar.operations;

import ash.nazg.columnar.config.ConfigurationParametersDummy;
import ash.nazg.spark.Operation;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import javassist.*;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.StringMemberValue;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple3;

import javax.xml.bind.DatatypeConverter;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;
import java.util.*;

@SuppressWarnings("unused")
public class DigestOperation extends Operation {
    public static final String VERB = "digest";

    private static List<String> KNOWN_COLUMNS = new ArrayList<>();

    private static Class augmented = null;

    private Tuple3<Integer, String, String>[] outputCols;
    private String outputName;
    private String inputName;
    private char inputDelimiter;

    public DigestOperation() throws Exception {
        if (augmented == null) {
            String digest = MessageDigest.class.getSimpleName();

            ClassPool classPool = ClassPool.getDefault();
            classPool.insertClassPath(new LoaderClassPath(getClass().getClassLoader()));
            CtClass cpClass = classPool.get(ConfigurationParametersDummy.class.getCanonicalName());
            String cpName = DigestOperation.class.getPackage().getName().replace(".operations", ".config") + ".ConfigurationParameters";
            cpClass.setName(cpName);

            CtClass stringClass = classPool.get(String.class.getCanonicalName());

            MessageDigest md5 = MessageDigest.getInstance("MD5");

            Provider[] providers = Security.getProviders();
            for (Provider provider : providers) {
                Set<Provider.Service> services = provider.getServices();
                for (Provider.Service service : services) {
                    if (service.getType().equalsIgnoreCase(digest)) {
                        String providerName = provider.getName();
                        String algorithm = service.getAlgorithm();

                        CtField genAlgo = new CtField(stringClass, "GEN_" + DatatypeConverter.printHexBinary(md5.digest((providerName + "_" + algorithm).getBytes())), cpClass);
                        ConstPool constPool = genAlgo.getFieldInfo().getConstPool();
                        Annotation ann = new Annotation(Description.class.getCanonicalName(), constPool);
                        ann.addMemberValue("value", new StringMemberValue("Provider " + providerName + " algorithm " + algorithm, constPool));
                        AnnotationsAttribute attr = new AnnotationsAttribute(constPool, AnnotationsAttribute.visibleTag);
                        attr.setAnnotation(ann);
                        genAlgo.getFieldInfo().addAttribute(attr);
                        genAlgo.setModifiers(Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL);
                        cpClass.addField(genAlgo, CtField.Initializer.constant("_" + providerName + "_" + algorithm + "_*"));

                        KNOWN_COLUMNS.add("_" + providerName + "_" + algorithm + "_*");
                    }
                }
            }

            augmented = cpClass.toClass();
        }
    }

    @Override
    @Description("This operation calculates cryptographic digest(s) for given input column(s)," +
            " by any algorithm provided by Java platform. Possible generated column list is dynamic," +
            " while each column name follows the convention of _PROVIDER_ALGORITHM_source.column." +
            " Default PROVIDER is SUN, and ALGORITHMs are MD2, MD5, SHA, SHA-224, SHA-256, SHA-384 and SHA-512")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                KNOWN_COLUMNS
                        )
                )
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setConfig(OperationConfig config) throws InvalidConfigValueException {
        super.setConfig(config);

        inputName = describedProps.inputs.get(0);
        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);
        outputName = describedProps.outputs.get(0);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);
        String[] outputColumns = dataStreamsProps.outputColumns.get(outputName);

        outputCols = new Tuple3[outputColumns.length];

        checkOutputColumns:
        for (int i = 0; i < outputColumns.length; i++) {
            String outputCol = outputColumns[i];

            if (inputColumns.containsKey(outputCol)) {
                outputCols[i] = new Tuple3<>(inputColumns.get(outputCol), null, null);
            } else {
                for (String prefix : KNOWN_COLUMNS) {
                    if (outputCol.startsWith(prefix.substring(0, prefix.length() - 2))) {
                        String[] columnRef = outputCol.split("_", 4);
                        int colNo = inputColumns.get(columnRef[3]);

                        outputCols[i] = new Tuple3<>(-colNo - 1, columnRef[1], columnRef[2]);
                        continue checkOutputColumns;
                    }
                }

                throw new InvalidConfigValueException("Output column '" + outputCol + "' doesn't reference input nor can be generated in the operation '" + name + "'");
            }
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final Tuple3<Integer, String, String>[] _outputCols = outputCols;

        JavaRDD<Text> output = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                            CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter)
                                    .build();

                            List<Text> ret = new ArrayList<>();
                            while (it.hasNext()) {
                                Object o = it.next();
                                String l = o instanceof String ? (String) o : String.valueOf(o);
                                String[] row = parser.parseLine(l);

                                String[] acc = new String[row.length + 1];
                                for (int i = 0; i < _outputCols.length; i++) {
                                    int col = _outputCols[i]._1();
                                    if (col >= 0) {
                                        acc[i] = row[col];
                                    } else {
                                        MessageDigest md = MessageDigest.getInstance(_outputCols[i]._3(), _outputCols[i]._2());

                                        acc[i] = DatatypeConverter.printHexBinary(md.digest(row[-1 - col].getBytes()));
                                    }
                                }

                                StringWriter buffer = new StringWriter();
                                CSVWriter writer = new CSVWriter(buffer, _inputDelimiter,
                                        CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                                writer.writeNext(acc, false);
                                writer.close();

                                ret.add(new Text(buffer.toString()));

                            }

                            return ret.iterator();
                        }
                );

        return Collections.singletonMap(outputName, output);
    }
}
