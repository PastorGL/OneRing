/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.metadata.AdapterMeta;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopInput extends InputAdapter {
    protected static final String MAX_RECORD_SIZE = "max.record.size";
    protected static final String SCHEMA = "schema";

    protected int partCount;
    protected String[] inputSchema;
    protected String[] dsColumns;
    protected char dsDelimiter;

    protected int maxRecordSize;

    protected int numOfExecutors;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("Hadoop", "Default input adapter that utilizes available Hadoop FileSystems." +
                " Supports text, text-based columnar (CSV/TSV), and Parquet files, optionally compressed",
                HadoopStorage.PATH_PATTERN,

                new DefinitionMetaBuilder()
                        .def(MAX_RECORD_SIZE, "Max record size, bytes", Integer.class, "1048576",
                                "By default, 1M")
                        .def(SCHEMA, "Loose schema of input records (just column of field names," +
                                        " optionally with placeholders to skip some, denoted by underscores _)",
                                String[].class, null, "By default, don't set the schema." +
                                        " Depending of source file type, built-in schema may be used")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        inputSchema = inputResolver.definition(SCHEMA);

        dsColumns = dsResolver.rawInputColumns(dsName);
        dsDelimiter = dsResolver.inputDelimiter(dsName);

        maxRecordSize = inputResolver.definition(MAX_RECORD_SIZE);

        partCount = Math.max(dsResolver.inputParts(dsName), 1);

        int executors = Integer.parseInt(context.getConf().get("spark.executor.instances", "-1"));
        numOfExecutors = (executors <= 0) ? 1 : (int) Math.ceil(executors * 0.8);
        numOfExecutors = Math.max(numOfExecutors, 1);

        if (partCount <= 0) {
            partCount = numOfExecutors;
        }
    }

    @Override
    public JavaRDD<Text> load(String globPattern) {
        // path, regex
        List<Tuple2<String, String>> splits = HadoopStorage.srcDestGroup(globPattern);

        // files
        List<String> discoveredFiles = context.parallelize(splits, numOfExecutors)
                .flatMap(srcDestGroup -> {
                    List<String> files = new ArrayList<>();
                    try {
                        Path srcPath = new Path(srcDestGroup._1());

                        Configuration conf = new Configuration();

                        FileSystem srcFS = srcPath.getFileSystem(conf);
                        RemoteIterator<LocatedFileStatus> srcFiles = srcFS.listFiles(srcPath, true);

                        Pattern pattern = Pattern.compile(srcDestGroup._2());

                        while (srcFiles.hasNext()) {
                            String srcFile = srcFiles.next().getPath().toString();

                            Matcher m = pattern.matcher(srcFile);
                            if (m.matches()) {
                                files.add(srcFile);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Exception while enumerating files to copy: " + e.getMessage());
                        e.printStackTrace(System.err);
                        System.exit(13);
                    }

                    return files.iterator();
                })
                .collect();

        System.out.println("Discovered Hadoop FileSystem files:");
        discoveredFiles.forEach(System.out::println);

        int countOfFiles = discoveredFiles.size();

        int groupSize = countOfFiles / partCount;
        if (groupSize <= 0) {
            groupSize = 1;
        }

        List<List<String>> partNum = new ArrayList<>();
        Lists.partition(discoveredFiles, groupSize).forEach(p -> partNum.add(new ArrayList<>(p)));

        FlatMapFunction<List<String>, Text> inputFunction = new InputFunction(inputSchema, dsColumns, dsDelimiter, maxRecordSize);

        return context.parallelize(partNum, partNum.size())
                .flatMap(inputFunction);
    }
}
