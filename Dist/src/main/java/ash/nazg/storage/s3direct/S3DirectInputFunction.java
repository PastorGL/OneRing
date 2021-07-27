package ash.nazg.storage.s3direct;

import ash.nazg.storage.hadoop.HadoopStorage;
import ash.nazg.storage.hadoop.InputFunction;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.util.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.security.MessageDigest;

public class S3DirectInputFunction extends InputFunction {
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;

    private final String _bucket;
    private final Path _tmp;

    public S3DirectInputFunction(String[] schema, String[] columns, char delimiter, int bufferSize, String endpoint, String region, String accessKey, String secretKey, String bucket, String tmp) {
        super(schema, columns, delimiter, bufferSize);

        this.endpoint = endpoint;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;

        this._bucket = bucket;
        this._tmp = new Path(tmp);
    }

    @Override
    protected InputStream decorateInputStream(Configuration conf, String inputFile) throws Exception {
        String suffix = HadoopStorage.suffix(inputFile);

        AmazonS3 _s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);
        InputStream inputStream = _s3.getObject(_bucket, inputFile).getObjectContent();

        if ("parquet".equalsIgnoreCase(suffix)) {
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            String pathHash = DatatypeConverter.printHexBinary(md5.digest((inputFile).getBytes()));

            Path localPath = new Path(_tmp, pathHash);

            FileSystem tmpFs = localPath.getFileSystem(conf);
            if (!tmpFs.exists(localPath)) {
                FSDataOutputStream fso = tmpFs.create(localPath, false);

                IOUtils.copy(inputStream, fso);
                fso.close();
                tmpFs.deleteOnExit(localPath);
            }

            inputStream = getParquetInputStream(conf, localPath.toString());
        } else {
            inputStream = getTextInputStream(conf, inputStream, HadoopStorage.Codec.lookup(suffix));
        }

        return inputStream;
    }
}
