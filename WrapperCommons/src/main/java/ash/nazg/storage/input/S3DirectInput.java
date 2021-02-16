/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.input;

import ash.nazg.config.DataStreamsConfig;
import ash.nazg.config.PropertiesConfig;
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
import org.sparkproject.guava.collect.Iterables;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
        accessKey = wrapperConfig.getInputProperty("access.key", inputName, null);
        secretKey = wrapperConfig.getInputProperty("secret.key", inputName, null);

        partCount = Math.max(wrapperConfig.inputParts(inputName), 1);

        DataStreamsConfig adapterConfig = new DataStreamsConfig(wrapperConfig.getLayerProperties(WrapperConfig.DS_PREFIX), Collections.singleton(inputName), Collections.singleton(inputName), null, null, null);
    }

    @Override
    public void setContext(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public JavaRDDLike load(String path) {
        Matcher m = PATTERN.matcher(path);
        m.matches();
        String bucket = m.group(1);
        String keyPrefix = m.group(2);

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard()
                .enableForceGlobalBucketAccess();
        if ((accessKey != null) && (secretKey != null)) {
            s3ClientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }

        AmazonS3 s3 = s3ClientBuilder.build();

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(keyPrefix);

        List<String> s3FileKeys = s3.listObjects(request).getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());

        final String _accessKey = accessKey;
        final String _secretKey = secretKey;
        final String _bucket = bucket;

        JavaRDDLike rdd = ctx.parallelize(s3FileKeys, partCount)
                .mapPartitions(it -> {
                    AmazonS3ClientBuilder s3cb = AmazonS3ClientBuilder.standard()
                            .enableForceGlobalBucketAccess();
                    if ((_accessKey != null) && (_secretKey != null)) {
                        s3cb.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(_accessKey, _secretKey)));
                    }
                    AmazonS3 _s3 = s3cb.build();

                    Stream<String> lines = null;
                    while (it.hasNext()) {
                        String key = it.next();

                        Stream<String> file = new BufferedReader(new InputStreamReader(_s3.getObject(_bucket, key).getObjectContent(), StandardCharsets.UTF_8.name())).lines();
                        if ((lines == null)) {
                            lines = file;
                        } else {
                            lines = Stream.concat(lines, file);
                        }
                    }

                    return lines.iterator();
                });

        return rdd;
    }
}
