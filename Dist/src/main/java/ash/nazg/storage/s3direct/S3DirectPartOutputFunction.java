package ash.nazg.storage.s3direct;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import ash.nazg.storage.hadoop.FileStorage;
import ash.nazg.storage.hadoop.PartOutputFunction;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.regex.Matcher;

public class S3DirectPartOutputFunction extends PartOutputFunction {
    private static final int BUFFER_SIZE = 5 * 1024 * 1024;
    private final String accessKey;
    private final String secretKey;

    private final String contentType;
    private final String endpoint;
    private final String region;
    private final Path _tmp;

    public S3DirectPartOutputFunction(String _name, String outputPath, String codec, String[] _columns, char _delimiter, String endpoint, String region, String accessKey, String secretKey, String tmpDir, String contentType) {
        super(_name, outputPath, codec, _columns, _delimiter);

        this.endpoint = endpoint;
        this.region = region;
        this.secretKey = secretKey;
        this.accessKey = accessKey;
        this.contentType = contentType;

        this._tmp = new Path(tmpDir);
    }

    @Override
    protected OutputStream createOutputStream(Configuration conf, int idx, Iterator<Text> it) throws Exception {
        String suffix = FileStorage.suffix(outputPath);

        Matcher m = S3DirectStorage.PATTERN.matcher(outputPath);
        m.matches();

        final String bucket = m.group(1);
        String key = m.group(2);

        AmazonS3 _s3 = S3DirectStorage.get(endpoint, region, accessKey, secretKey);

        String partName = String.format("part-%05d", idx);
        if (FileStorage.CODECS.containsKey(codec)) {
            partName += "." + codec;
        }
        if ("parquet".equalsIgnoreCase(suffix)) {
            partName += ".parquet";
            key = key.substring(0, key.lastIndexOf("/"));
        }

        StreamTransferManager stm = new StreamTransferManager(bucket, key + "/" + partName, _s3) {
            @Override
            public void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
                ObjectMetadata om = new ObjectMetadata();
                om.setContentType(contentType);
                request.setObjectMetadata(om);
            }
        };

        MultiPartOutputStream outputStream = stm.numStreams(1)
                .numUploadThreads(1)
                .queueCapacity(1)
                .partSize(15)
                .getMultiPartOutputStreams().get(0);

        if ("parquet".equalsIgnoreCase(suffix)) {
            Path partPath = new Path(_tmp + "/" + _name + "/" + partName);

            writeToParquetFile(conf, partPath, it);

            FileSystem tmpFs = partPath.getFileSystem(conf);
            InputStream inputStream = tmpFs.open(partPath, BUFFER_SIZE);

            int len;
            for (byte[] buffer = new byte[BUFFER_SIZE]; (len = inputStream.read(buffer)) > 0; ) {
                outputStream.write(buffer, 0, len);
            }
            outputStream.close();
            tmpFs.delete(partPath, false);

            return null;
        } else {
            return writeToTextFile(conf, outputStream);
        }
    }
}
