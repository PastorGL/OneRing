package ash.nazg.dist;

import ash.nazg.config.InvalidConfigValueException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.compress.*;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple3;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CopyFilesFunction implements VoidFunction<Tuple3<String, String, String>> {
    private static final Map<String, Class<? extends CompressionCodec>> CODECS = new HashMap<>();

    static {
        CODECS.put("gz", GzipCodec.class);
        CODECS.put("gzip", GzipCodec.class);
        CODECS.put("bz2", BZip2Codec.class);
        CODECS.put("snappy", SnappyCodec.class);
        CODECS.put("lz4", Lz4Codec.class);
    }

    private static final int BUFFER_SIZE = 15 * 1024 * 1024;

    private final Configuration conf;
    private final boolean deleteOnSuccess;
    private String codec;

    public CopyFilesFunction(Configuration conf, boolean deleteOnSuccess, String codec) {
        this.conf = conf;
        this.deleteOnSuccess = deleteOnSuccess;
        this.codec = codec;
    }

    private String getSuffix(String name) {
        if (name != null) {
            String[] parts = name.split("\\.");
            if (parts.length > 1) {
                return parts[parts.length - 1];
            }
        }

        return "";
    }

    private OutputStream decorateOutputStream(Path outputFilePath) throws Exception {
        FileSystem outputFs = outputFilePath.getFileSystem(conf);
        OutputStream outputStream = outputFs.create(outputFilePath);

        String suffix = getSuffix(outputFilePath.getName()).toLowerCase();
        if (CODECS.containsKey(suffix)) {
            Class<? extends CompressionCodec> cc = CODECS.get(suffix);
            CompressionCodec codec = cc.newInstance();
            ((Configurable) codec).setConf(conf);

            return codec.createOutputStream(outputStream);
        } else {
            return outputStream;
        }
    }

    private InputStream decorateInputStream(Path inputFilePath) throws Exception {
        FileSystem inputFs = inputFilePath.getFileSystem(conf);
        InputStream inputStream = inputFs.open(inputFilePath);

        String suffix = getSuffix(inputFilePath.getName()).toLowerCase();
        if (CODECS.containsKey(suffix)) {
            Class<? extends CompressionCodec> cc = CODECS.get(suffix);
            CompressionCodec codec = cc.newInstance();
            ((Configurable) codec).setConf(conf);

            return codec.createInputStream(inputStream);
        } else {
            return inputStream;
        }
    }

    public void mergeAndCopyFiles(List<String> inputFiles, String outputFile) throws Exception {
        Path outputFilePath = new Path(outputFile);

        try (OutputStream outputStream = decorateOutputStream(outputFilePath)) {
            for (String inputFile : inputFiles) {
                Path inputFilePath = new Path(inputFile);

                try (InputStream inputStream = decorateInputStream(inputFilePath)) {
                    int len;
                    for (byte[] buffer = new byte[BUFFER_SIZE]; (len = inputStream.read(buffer)) > 0; ) {
                        outputStream.write(buffer, 0, len);
                    }
                } catch (Exception e) {
                    FileSystem outFs = outputFilePath.getFileSystem(conf);
                    outFs.delete(outputFilePath, true);

                    throw e;
                }
            }
        }
    }

    @Override
    public void call(Tuple3<String, String, String> srcDestGroup) {
        final String src = srcDestGroup._1();
        String dest = srcDestGroup._2();
        final String groupBy = srcDestGroup._3();
        try {
            Path srcPath = new Path(src);

            FileSystem srcFS = srcPath.getFileSystem(conf);
            RemoteIterator<LocatedFileStatus> srcFiles = srcFS.listFiles(srcPath, true);

            Pattern pattern = Pattern.compile(groupBy);

            List<String> ret = new ArrayList<>();
            HashSet<String> codecs = new HashSet<>();
            while (srcFiles.hasNext()) {
                String srcFile = srcFiles.next().getPath().toString();

                Matcher m = pattern.matcher(srcFile);
                if (m.matches()) {
                    ret.add(srcFile);
                }

                codecs.add(getSuffix(srcFile));
            }

            if (codecs.isEmpty()) {
                throw new InvalidConfigValueException("No files to copy");
            }

            if ("keep".equalsIgnoreCase(codec)) {
                if ((codecs.size() > 1)) {
                    throw new InvalidConfigValueException("Cannot keep compression scheme for input files with different compression schemes");
                } else {
                    codec = codecs.toArray(new String[0])[0];
                }
            }

            if (CODECS.containsKey(codec)) {
                dest += "." + codec;
            }

            mergeAndCopyFiles(ret, dest);

            if (deleteOnSuccess) {
                srcFS.delete(srcPath, true);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(14);
        }
    }
}
