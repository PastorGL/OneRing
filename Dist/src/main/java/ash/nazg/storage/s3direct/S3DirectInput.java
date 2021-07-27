/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.s3direct;

import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.hadoop.HadoopInput;
import ash.nazg.storage.metadata.AdapterMeta;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ash.nazg.storage.s3direct.S3DirectStorage.*;

@SuppressWarnings("unused")
public class S3DirectInput extends HadoopInput {
    private String accessKey;
    private String secretKey;
    private String endpoint;
    private String region;
    private String tmpDir;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("S3Direct", "Input adapter for any S3-compatible storage, based on Hadoop adapter",
                S3DirectStorage.PATH_PATTERN,

                new DefinitionMetaBuilder()
                        .def(MAX_RECORD_SIZE, "Max record size, bytes", Integer.class, "1048576",
                                "By default, 1M")
                        .def(SCHEMA, "Loose schema of input records (just column of field names," +
                                        " optionally with placeholders to skip some, denoted by underscores _)",
                                String[].class, null, "By default, don't set the schema." +
                                        " Depending of source type, built-in schema may be used")
                        .def(S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                                " the endpoint from client's standard profile")
                        .def(S3D_REGION, "S3 region", null, "By default, try to discover" +
                                " the region from client's standard profile")
                        .build()
        );
    }

    @Override
    protected void configure() {
        super.configure();

        accessKey = inputResolver.definition(S3D_ACCESS_KEY);
        secretKey = inputResolver.definition(S3D_SECRET_KEY);
        endpoint = inputResolver.definition(S3D_ENDPOINT);
        region = inputResolver.definition(S3D_REGION);

        tmpDir = distResolver.get("tmp");
    }

    @Override
    public JavaRDD load(String s3path) {
        Matcher m = Pattern.compile(S3DirectStorage.PATH_PATTERN).matcher(s3path);
        m.matches();
        String bucket = m.group(1);
        String keyPrefix = m.group(2);

        AmazonS3 s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(keyPrefix);

        List<String> discoveredFiles = s3.listObjects(request).getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());

        System.out.println("Discovered S3 objects:");
        discoveredFiles.forEach(System.out::println);

        int countOfFiles = discoveredFiles.size();

        int groupSize = countOfFiles / partCount;
        if (groupSize <= 0) {
            groupSize = 1;
        }

        List<List<String>> partNum = new ArrayList<>();
        Lists.partition(discoveredFiles, groupSize).forEach(p -> partNum.add(new ArrayList<>(p)));

        FlatMapFunction<List<String>, Text> inputFunction = new S3DirectInputFunction(inputSchema, dsColumns, dsDelimiter, maxRecordSize,
                endpoint, region, accessKey, secretKey, bucket, tmpDir);

        return context.parallelize(partNum, partNum.size())
                .flatMap(inputFunction);
    }
}
