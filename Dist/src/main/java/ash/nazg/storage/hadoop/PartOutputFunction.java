package ash.nazg.storage.hadoop;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.api.java.function.Function2;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class PartOutputFunction implements Function2<Integer, Iterator<Text>, Iterator<Void>> {
    protected final String _name;
    protected final String outputPath;
    protected final HadoopStorage.Codec codec;
    protected final String[] _columns;
    protected final char _delimiter;

    public PartOutputFunction(String _name, String outputPath, HadoopStorage.Codec codec, String[] _columns, char _delimiter) {
        this.outputPath = outputPath;
        this.codec = codec;
        this._columns = _columns;
        this._name = _name;
        this._delimiter = _delimiter;
    }

    @Override
    public Iterator<Void> call(Integer idx, Iterator<Text> it) {
        Configuration conf = new Configuration();

        try {
            OutputStream os = createOutputStream(conf, idx, it);

            if (os != null) {
                while (it.hasNext()) {
                    os.write(String.valueOf(it.next()).getBytes(StandardCharsets.UTF_8));
                }

                os.close();
            }
        } catch (Exception e) {
            System.err.println("Exception while writing records: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(15);
        }

        return Collections.emptyIterator();
    }

    protected OutputStream createOutputStream(Configuration conf, int idx, Iterator<Text> it) throws Exception {
        String suffix = HadoopStorage.suffix(outputPath);

        if ("parquet".equalsIgnoreCase(suffix)) {
            String partName = outputPath.substring(0, outputPath.lastIndexOf(".")) + "/" + String.format("part-%05d", idx)
                    + ((codec != HadoopStorage.Codec.NONE) ? "." + codec.name().toLowerCase() : "") + ".parquet";

            writeToParquetFile(conf, new Path(partName), it);

            return null;
        } else {
            String partName = outputPath + "/" + String.format("part-%05d", idx)
                    + ((codec != HadoopStorage.Codec.NONE) ? "." + codec.name().toLowerCase() : "");

            Path partPath = new Path(partName);

            FileSystem outputFs = partPath.getFileSystem(conf);
            outputFs.setVerifyChecksum(false);
            outputFs.setWriteChecksum(false);
            OutputStream outputStream = outputFs.create(partPath);

            return writeToTextFile(conf, outputStream);
        }
    }

    protected void writeToParquetFile(Configuration conf, Path partPath, Iterator<Text> it) throws Exception {
        List<Type> types = new ArrayList<>();
        for (String col : _columns) {
            types.add(Types.primitive(BINARY, Type.Repetition.REQUIRED).as(stringType()).named(col));
        }
        MessageType schema = new MessageType(_name, types);

        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(partPath)
                .withConf(conf)
                .withType(schema)
                .withPageWriteChecksumEnabled(false);
        if (codec != HadoopStorage.Codec.NONE) {
            builder.withCompressionCodec(CompressionCodecName.fromCompressionCodec(codec.codec));
        }
        ParquetWriter<Group> writer = builder.build();

        CSVParser parser = new CSVParserBuilder().withSeparator(_delimiter).build();
        int numCols = _columns.length;
        while (it.hasNext()) {
            String line = String.valueOf(it.next());

            String[] ll = parser.parseLine(line);
            Group group = new SimpleGroup(schema);

            for (int i = 0; i < numCols; i++) {
                group.add(i, ll[i]);
            }

            writer.write(group);
        }

        writer.close();
    }

    protected OutputStream writeToTextFile(Configuration conf, OutputStream outputStream) throws Exception {
        if (codec != HadoopStorage.Codec.NONE) {
            Class<? extends CompressionCodec> cc = codec.codec;
            CompressionCodec codec = cc.newInstance();
            ((Configurable) codec).setConf(conf);

            return codec.createOutputStream(outputStream);
        } else {
            return outputStream;
        }
    }
}
