/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.s3direct;

import ash.nazg.config.tdl.Description;
import ash.nazg.storage.hadoop.HadoopInput;
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

@SuppressWarnings("unused")
public class S3DirectInput extends HadoopInput {
    private static final Pattern PATTERN = Pattern.compile("^s3d://([^/]+)/(.+)");

    private String accessKey;
    private String secretKey;
    private String endpoint;
    private String region;
    private String tmpDir;

    @Description("S3 Direct adapter for any S3-compatible storage")
    public Pattern proto() {
        return PATTERN;
    }

    @Override
    protected void configure() {
        super.configure();

        accessKey = inputResolver.get("s3d.access.key." + name);
        secretKey = inputResolver.get("s3d.secret.key." + name);
        endpoint = inputResolver.get("s3d.endpoint." + name);
        region = inputResolver.get("s3d.region." + name);

        tmpDir = distResolver.get("tmp");
    }

    @Override
    public JavaRDD load(String path) {
        Matcher m = PATTERN.matcher(path);
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
