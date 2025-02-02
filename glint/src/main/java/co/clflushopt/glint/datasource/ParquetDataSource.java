package co.clflushopt.glint.datasource;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import co.clflushopt.glint.types.ArrowFieldVector;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;
import co.clflushopt.glint.types.SchemaConverter;

public class ParquetDataSource implements DataSource {
    private final String filename;
    private final BufferAllocator allocator;
    private final Schema schema;

    public ParquetDataSource(String filename) {
        this.filename = "file:" + filename;
        this.allocator = new RootAllocator();
        this.schema = new Schema(List.of());
    }

    public ParquetDataSource(String filename, Schema schema) {
        this.filename = "file:" + filename;
        this.allocator = new RootAllocator();
        this.schema = schema;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Iterable<RecordBatch> scan(List<String> projection) {
        return () -> new ParquetBatchIterator(filename, projection, allocator);
    }
}

class ParquetBatchIterator implements Iterator<RecordBatch> {
    private final ArrowReader reader;
    private final Dataset dataset;
    private final DatasetFactory factory;
    private RecordBatch nextBatch;
    private boolean closed = false;

    public ParquetBatchIterator(String filename, List<String> projection,
            BufferAllocator allocator) {
        try {
            ScanOptions scanOptions;
            // Set up scanning options
            if (!projection.isEmpty()) {
                String[] columns = projection.toArray(new String[0]);
                scanOptions = new ScanOptions(32768, Optional.of(columns));
            } else {
                scanOptions = new ScanOptions(32768);
            }

            // Create the scanner
            this.factory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, filename);
            this.dataset = factory.finish();
            Scanner scanner = dataset.newScan(scanOptions);
            this.reader = scanner.scanBatches();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Parquet scanner", e);
        }
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        try {
            if (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                Schema schema = SchemaConverter.fromArrow(root.getSchema());
                nextBatch = new RecordBatch(schema, root.getFieldVectors().stream()
                        .map(ArrowFieldVector::new).collect(Collectors.toList()));
                return true;
            } else {
                close();
                return false;
            }
        } catch (Exception e) {
            close();
            throw new RuntimeException("Error reading next batch", e);
        }
    }

    @Override
    public RecordBatch next() {
        if (nextBatch == null) {
            throw new NoSuchElementException();
        }
        RecordBatch batch = nextBatch;
        nextBatch = null;
        return batch;
    }

    private void close() {
        if (!closed) {
            try {
                reader.close();
                dataset.close();
                factory.close();
                closed = true;
            } catch (Exception e) {
                throw new RuntimeException("Error closing resources", e);
            }
        }
    }
}