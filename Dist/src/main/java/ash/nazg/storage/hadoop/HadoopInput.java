/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.StorageAdapter;
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
    protected int partCount;
    protected String[] inputSchema;
    protected String[] dsColumns;
    protected char dsDelimiter;
    protected int maxRecordSize;

    protected static final int DEFAULT_SIZE = 1024 * 1024;
    protected int numOfExecutors;

    @Description("Default Storage that utilizes Hadoop filesystems")
    public Pattern proto() {
        return StorageAdapter.PATH_PATTERN;
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        inputSchema = inputResolver.getArray("schema." + name);
        dsColumns = dsResolver.rawInputColumns(name);
        dsDelimiter = dsResolver.inputDelimiter(name);

        maxRecordSize = Integer.parseInt(inputResolver.get("max.record.size", String.valueOf(DEFAULT_SIZE)));

        partCount = Math.max(dsResolver.inputParts(name), 1);

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
        List<Tuple2<String, String>> splits = FileStorage.srcDestGroup(globPattern);

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
