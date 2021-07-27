/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.s3direct;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.hadoop.HadoopOutput;
import ash.nazg.storage.hadoop.HadoopStorage;
import ash.nazg.storage.metadata.AdapterMeta;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

import static ash.nazg.storage.s3direct.S3DirectStorage.*;

@SuppressWarnings("unused")
public class S3DirectOutput extends HadoopOutput {
    static final String CONTENT_TYPE = "content.type";

    private String accessKey;
    private String secretKey;

    private String contentType;
    private String endpoint;
    private String region;
    private String tmpDir;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("S3Direct", "Multipart output adapter for any S3-compatible storage, based on Hadoop adapter",
                S3DirectStorage.PATH_PATTERN,

                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", HadoopStorage.Codec.class, HadoopStorage.Codec.NONE.name(),
                                "By default, use no compression")
                        .def(S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                                " the endpoint from client's standard profile")
                        .def(S3D_REGION, "S3 region", null, "By default, try to discover" +
                                " the region from client's standard profile")
                        .def(CONTENT_TYPE, "Content type for objects", "text/csv", "By default," +
                                " content type is CSV")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        super.configure();

        accessKey = outputResolver.definition(S3D_ACCESS_KEY);
        secretKey = outputResolver.definition(S3D_SECRET_KEY);
        endpoint = outputResolver.definition(S3D_ENDPOINT);
        region = outputResolver.definition(S3D_REGION);

        contentType = outputResolver.definition(CONTENT_TYPE);

        tmpDir = distResolver.get("tmp");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void save(String path, JavaRDD<Text> rdd) {
        Function2<Integer, Iterator<Text>, Iterator<Void>> outputFunction = new S3DirectPartOutputFunction(dsName, path, codec, columns, delimiter,
                endpoint, region, accessKey, secretKey, tmpDir, contentType);

        rdd.mapPartitionsWithIndex(outputFunction, true).count();
    }
}
