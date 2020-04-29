/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.input;

import ash.nazg.config.DataStreamsConfig;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.S3DirectAdapter;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public class S3DirectInput extends S3DirectAdapter implements InputAdapter {
    private JavaSparkContext ctx;
    private int partCount;

    @Override
    public void setProperties(String inputName, WrapperConfig wrapperConfig) {
        accessKey = wrapperConfig.getInputProperty("access.key", null);
        secretKey = wrapperConfig.getInputProperty("secret.key", null);

        partCount = wrapperConfig.inputParts(inputName);

        DataStreamsConfig adapterConfig = new DataStreamsConfig(wrapperConfig.getProperties(), Collections.singleton(inputName), Collections.singleton(inputName), null, null, null);
    }

    @Override
    public void setContext(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public JavaRDDLike load(String path) throws Exception {
        Matcher m = PATTERN.matcher(path);
        m.matches();
        String bucket = m.group(1);
        String key = m.group(2);

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard()
                .enableForceGlobalBucketAccess();
        if ((accessKey != null) && (secretKey != null)) {
            s3ClientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }

        AmazonS3 s3 = s3ClientBuilder.build();

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(key);

        List<String> s3FileKeys = s3.listObjects(request).getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());

        JavaRDD<Object> rdd = ctx.emptyRDD();

        for (String k : s3FileKeys) {
            Stream<String> lines = new BufferedReader(new InputStreamReader(s3.getObject(bucket, k).getObjectContent(), StandardCharsets.UTF_8.name())).lines();
            rdd = rdd.union(ctx.parallelize(lines.collect(Collectors.toList())));
        }

        return rdd.repartition(Math.max(partCount, 1));
    }
}
