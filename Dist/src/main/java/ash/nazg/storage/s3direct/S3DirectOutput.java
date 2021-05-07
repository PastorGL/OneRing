/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.s3direct;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.storage.hadoop.HadoopOutput;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class S3DirectOutput extends HadoopOutput {

    private String accessKey;
    private String secretKey;

    private String contentType;
    private String endpoint;
    private String region;
    private String tmpDir;

    @Description("S3 Direct adapter for any S3-compatible storage")
    public Pattern proto() {
        return S3DirectStorage.PATTERN;
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        super.configure();

        accessKey = outputResolver.get("s3d.access.key." + name);
        secretKey = outputResolver.get("s3d.secret.key." + name);
        endpoint = outputResolver.get("s3d.endpoint." + name);
        region = outputResolver.get("s3d.region." + name);

        contentType = outputResolver.get("s3d.content.type." + name, "text/csv");

        tmpDir = distResolver.get("tmp");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void save(String path, JavaRDD<Text> rdd) {
        Function2<Integer, Iterator<Text>, Iterator<Void>> outputFunction = new S3DirectPartOutputFunction(name, path, codec, columns, delimiter,
                endpoint, region, accessKey, secretKey, tmpDir, contentType);

        rdd.mapPartitionsWithIndex(outputFunction, true).count();
    }
}
