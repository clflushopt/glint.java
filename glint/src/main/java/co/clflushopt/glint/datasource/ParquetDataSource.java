package co.clflushopt.glint.datasource;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import co.clflushopt.glint.types.ArrowFieldVector;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;
import co.clflushopt.glint.types.SchemaConverter;

public class ParquetDataSource implements DataSource {
    private final String filename;

    public ParquetDataSource(String filename) {
        this.filename = filename;
    }

    @Override
    public Schema getSchema() {
        try (ParquetScan scan = new ParquetScan(filename, Collections.emptyList())) {
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = SchemaConverter
                    .fromParquet(scan.getSchema()).toArrow();
            return SchemaConverter.fromArrow(arrowSchema);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read schema from Parquet file", e);
        }
    }

    @SuppressWarnings("resource")
    @Override
    public Iterable<RecordBatch> scan(List<String> projection) {
        // Return an Iterable that creates a new ParquetScan each time iterator() is
        // called
        return () -> {
            try {
                return new ParquetScan(filename, projection).iterator();
            } catch (IOException e) {
                throw new RuntimeException("Failed to create ParquetScan", e);
            }
        };
    }
}

class ParquetScan implements AutoCloseable {
    private final ParquetFileReader reader;
    private final List<String> columns;
    private final org.apache.parquet.schema.MessageType schema;

    public ParquetScan(String filename, List<String> columns) throws IOException {
        this.columns = columns;
        this.reader = ParquetFileReader
                .open(HadoopInputFile.fromPath(new Path(filename), new Configuration()));
        this.schema = reader.getFooter().getFileMetaData().getSchema();
    }

    public Iterator<RecordBatch> iterator() {
        return new ParquetIterator(reader, columns);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public org.apache.parquet.schema.MessageType getSchema() {
        return schema;
    }
}

class ParquetIterator implements Iterator<RecordBatch> {
    private final ParquetFileReader reader;
    private final List<String> projectedColumns;
    private final org.apache.parquet.schema.MessageType schema;
    private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;
    private final org.apache.arrow.vector.types.pojo.Schema projectedArrowSchema;
    private RecordBatch batch;

    public ParquetIterator(ParquetFileReader reader, List<String> projectedColumns) {
        this.reader = reader;
        this.projectedColumns = projectedColumns;
        this.schema = reader.getFooter().getFileMetaData().getSchema();
        this.arrowSchema = SchemaConverter.fromParquet(schema).toArrow();

        if (projectedColumns.isEmpty()) {
            // Project all columns
            this.projectedArrowSchema = arrowSchema;
        } else {
            // Create projected schema
            List<org.apache.arrow.vector.types.pojo.Field> projectedFields = projectedColumns
                    .stream().map(
                            name -> arrowSchema.getFields().stream()
                                    .filter(f -> f.getName().equals(name)).findFirst()
                                    .orElseThrow(() -> new IllegalArgumentException(
                                            "Column not found: " + name)))
                    .collect(Collectors.toList());

            this.projectedArrowSchema = new org.apache.arrow.vector.types.pojo.Schema(
                    projectedFields);
        }

    }

    @Override
    public boolean hasNext() {
        batch = nextBatch();
        return batch != null;
    }

    @Override
    public RecordBatch next() {
        if (batch == null) {
            throw new NoSuchElementException();
        }
        RecordBatch result = batch;
        batch = null;
        return result;
    }

    private RecordBatch nextBatch() {
        try (PageReadStore pages = reader.readNextRowGroup()) {
            if (pages == null) {
                return null;
            }

            if (pages.getRowCount() > Integer.MAX_VALUE) {
                throw new IllegalStateException("Row count exceeds maximum integer value");
            }

            int rows = (int) pages.getRowCount();

            VectorSchemaRoot root = VectorSchemaRoot.create(projectedArrowSchema,
                    new RootAllocator(Long.MAX_VALUE));
            root.allocateNew();
            root.setRowCount(rows);

            Schema convertedSchema = SchemaConverter.fromArrow(projectedArrowSchema);

            return new RecordBatch(convertedSchema, root.getFieldVectors().stream()
                    .map(ArrowFieldVector::new).collect(Collectors.toList()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return batch;
    }
}