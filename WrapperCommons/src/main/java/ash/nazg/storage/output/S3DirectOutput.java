/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.output;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import ash.nazg.config.DataStreamsConfig;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.PropertiesConfig;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.storage.S3DirectAdapter;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.Iterators;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;

@SuppressWarnings("unused")
public class S3DirectOutput extends S3DirectAdapter implements OutputAdapter {
    private String contentType;

    @Override
    @SuppressWarnings("unchecked")
    public void save(String path, JavaRDDLike rdd) {
        Matcher m = PATTERN.matcher(path);
        m.matches();
        String bucket = m.group(1);
        String key = m.group(2);

        if (rdd instanceof JavaRDD) {
            rdd
                    .mapPartitionsWithIndex(new S3DirectWriteFunction(accessKey, secretKey, bucket, key, contentType), true)
                    .count();
        }
        if (rdd instanceof JavaPairRDD) {
            final String _delimiter = "" + delimiter;

            ((JavaPairRDD<Object, Object>) rdd)
                    .mapPartitionsWithIndex((idx, it) -> {
                        List ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2 v = it.next();

                            ret.add(new Text(v._1 + _delimiter + v._2));
                        }

                        return new S3DirectWriteFunction(accessKey, secretKey, bucket, key, contentType).call(idx, ret.iterator());
                    }, true)
                    .count();
        }
    }

    @Override
    public void setProperties(String outputName, WrapperConfig wrapperConfig) throws InvalidConfigValueException {
        accessKey = wrapperConfig.getOutputProperty("access.key", outputName, null);
        secretKey = wrapperConfig.getOutputProperty("secret.key", outputName, null);

        contentType = wrapperConfig.getOutputProperty("content.type", outputName, "text/csv");

        DataStreamsConfig adapterConfig = new DataStreamsConfig(wrapperConfig.getLayerProperties(WrapperConfig.DS_PREFIX), null, null, Collections.singleton(outputName), Collections.singleton(outputName), null);
        delimiter = adapterConfig.outputDelimiter(outputName);
    }

    public static class S3DirectWriteFunction implements Function2<Integer, Iterator<Object>, Iterator<Object>> {
        private final String _accessKey;
        private final String _secretKey;
        private final String _bucket;
        private final String _path;
        private final String _contentType;
        private transient AmazonS3 _client;

        private S3DirectWriteFunction(String accessKey, String secretKey, String bucket, String path, String contentType) {
            _accessKey = accessKey;
            _secretKey = secretKey;
            _bucket = bucket;
            _path = path;
            _contentType = contentType;
        }

        @Override
        public Iterator<Object> call(Integer partNumber, Iterator<Object> partition) {
            if (_client == null) {
                AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard()
                        .enableForceGlobalBucketAccess();
                if ((_accessKey != null) && (_secretKey != null)) {
                    s3ClientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(_accessKey, _secretKey)));
                }

                _client = s3ClientBuilder.build();
            }

            StreamTransferManager stm = new StreamTransferManager(_bucket, _path + "." + partNumber, _client) {
                @Override
                public void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
                    ObjectMetadata om = new ObjectMetadata();
                    om.setContentType(_contentType);
                    request.setObjectMetadata(om);
                }
            };

            MultiPartOutputStream stream = stm.numStreams(1)
                    .numUploadThreads(1)
                    .queueCapacity(1)
                    .partSize(15)
                    .getMultiPartOutputStreams().get(0);
            while (partition.hasNext()) {
                Object v = partition.next();

                byte[] buf = null;
                int len = 0;
                if (v instanceof String) {
                    String s = (String) v;
                    buf = s.getBytes();
                    len = buf.length;
                }
                if (v instanceof Text) {
                    Text t = (Text) v;
                    buf = t.getBytes();
                    len = t.getLength();
                }

                stream.write(buf, 0, len);
            }
            stream.close();
            stm.complete();

            return Iterators.emptyIterator();
        }
    }
}
