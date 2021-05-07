package ash.nazg.storage.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.IntStream;

public class InputFunction implements FlatMapFunction<List<String>, Text> {
    final protected String[] _schema;
    final protected String[] _columns;
    final protected char _delimiter;
    final protected int _bufferSize;

    public InputFunction(String[] schema, String[] columns, char delimiter, int bufferSize) {
        _schema = schema;
        _columns = columns;
        _delimiter = delimiter;
        _bufferSize = bufferSize;
    }

    @Override
    public Iterator<Text> call(List<String> src) {
        ArrayList<Text> ret = new ArrayList<>();

        Configuration conf = new Configuration();
        try {
            for (String inputFile : src) {
                InputStream inputStream = decorateInputStream(conf, inputFile);

                int len;
                for (byte[] buffer = new byte[_bufferSize]; (len = inputStream.read(buffer)) > 0; ) {
                    String str = new String(buffer, 0, len, StandardCharsets.UTF_8);
                    ret.add(new Text(str));
                }
            }
        } catch (Exception e) {
            System.err.println("Exception while reading records: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(14);
        }

        return ret.iterator();
    }

    protected InputStream decorateInputStream(Configuration conf, String inputFile) throws Exception {
        InputStream inputStream;

        String suffix = FileStorage.suffix(inputFile);

        if ("parquet".equalsIgnoreCase(suffix)) {
            inputStream = getParquetInputStream(conf, inputFile);
        } else {
            Path inputFilePath = new Path(inputFile);
            FileSystem inputFs = inputFilePath.getFileSystem(conf);
            inputStream = inputFs.open(inputFilePath);

            inputStream = getTextInputStream(conf, inputStream, suffix);
        }

        return inputStream;
    }

    protected InputStream getParquetInputStream(Configuration conf, String inputFile) throws Exception {
        Path inputFilePath = new Path(inputFile);

        ParquetMetadata readFooter = ParquetFileReader.readFooter(HadoopInputFile.fromPath(inputFilePath, conf), ParquetMetadataConverter.NO_FILTER);
        MessageType schema = readFooter.getFileMetaData().getSchema();

        int[] fieldOrder;
        if (_columns != null) {
            fieldOrder = new int[_columns.length];

            for (int i = 0; i < _columns.length; i++) {
                String column = _columns[i];
                fieldOrder[i] = schema.getFieldIndex(column);
            }
        } else {
            fieldOrder = IntStream.range(0, schema.getFieldCount()).toArray();
        }

        GroupReadSupport readSupport = new GroupReadSupport();
        readSupport.init(conf, null, schema);
        ParquetReader<Group> reader = ParquetReader.builder(readSupport, inputFilePath).build();

        return new ParquetRecordInputStream(reader, fieldOrder, _delimiter);
    }

    protected InputStream getTextInputStream(Configuration conf, InputStream inputStream, String codec) throws Exception {
        codec = codec.toLowerCase();
        if (FileStorage.CODECS.containsKey(codec)) {
            Class<? extends CompressionCodec> codecClass = FileStorage.CODECS.get(codec);
            CompressionCodec cc = codecClass.newInstance();
            ((Configurable) cc).setConf(conf);

            inputStream = cc.createInputStream(inputStream);
        }

        if ((_schema != null) || (_columns != null)) {
            int[] columnOrder;

            if (_schema != null) {
                if (_columns == null) {
                    columnOrder = IntStream.range(0, _schema.length).toArray();
                } else {
                    Map<String, Integer> schema = new HashMap<>();
                    for (int i = 0; i < _schema.length; i++) {
                        schema.put(_schema[i], i);
                    }

                    Map<Integer, String> columns = new HashMap<>();
                    for (int i = 0; i < _columns.length; i++) {
                        columns.put(i, _columns[i]);
                    }

                    columnOrder = new int[_columns.length];
                    for (int i = 0; i < _columns.length; i++) {
                        columnOrder[i] = schema.get(columns.get(i));
                    }
                }
            } else {
                columnOrder = IntStream.range(0, _columns.length).toArray();
            }

            inputStream = new CSVRecordInputStream(inputStream, columnOrder, _delimiter);
        }

        return inputStream;
    }
}
