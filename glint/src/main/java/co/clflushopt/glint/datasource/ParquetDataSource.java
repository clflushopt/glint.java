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
/*
 * public class ParquetDataSource implements DataSource { private final String
 * filename;
 *
 * public ParquetDataSource(String filename) { this.filename = filename; }
 *
 * @Override public Schema getSchema() { try (ParquetScan scan = new
 * ParquetScan(filename, Collections.emptyList())) {
 * org.apache.arrow.vector.types.pojo.Schema arrowSchema = SchemaConverter
 * .fromParquet(scan.getSchema()).toArrow(); return
 * SchemaConverter.fromArrow(arrowSchema); } catch (IOException e) { throw new
 * RuntimeException("Failed to read schema from Parquet file", e); } }
 *
 * @SuppressWarnings("resource")
 *
 * @Override public Iterable<RecordBatch> scan(List<String> projection) { //
 * Return an Iterable that creates a new ParquetScan each time iterator() is //
 * called return () -> { try { return new ParquetScan(filename,
 * projection).iterator(); } catch (IOException e) { throw new
 * RuntimeException("Failed to create ParquetScan", e); } }; } }
 *
 * class ParquetScan implements AutoCloseable { private final ParquetFileReader
 * reader; private final List<String> columns; private final
 * org.apache.parquet.schema.MessageType schema;
 *
 * public ParquetScan(String filename, List<String> columns) throws IOException
 * { this.columns = columns; this.reader = ParquetFileReader
 * .open(HadoopInputFile.fromPath(new Path(filename), new Configuration()));
 * this.schema = reader.getFooter().getFileMetaData().getSchema(); }
 *
 * public Iterator<RecordBatch> iterator() { return new ParquetIterator(reader,
 * columns); }
 *
 * @Override public void close() throws IOException { reader.close(); }
 *
 * public org.apache.parquet.schema.MessageType getSchema() { return schema; } }
 *
 * class ParquetIterator implements Iterator<RecordBatch> { private final
 * ParquetFileReader reader; private final List<String> projectedColumns;
 * private final org.apache.parquet.schema.MessageType schema; private final
 * org.apache.arrow.vector.types.pojo.Schema arrowSchema; private final
 * org.apache.arrow.vector.types.pojo.Schema projectedArrowSchema; private
 * RecordBatch batch;
 *
 * public ParquetIterator(ParquetFileReader reader, List<String>
 * projectedColumns) { this.reader = reader; this.projectedColumns =
 * projectedColumns; this.schema =
 * reader.getFooter().getFileMetaData().getSchema(); this.arrowSchema =
 * SchemaConverter.fromParquet(schema).toArrow();
 *
 * if (projectedColumns.isEmpty()) { // Project all columns
 * this.projectedArrowSchema = arrowSchema; } else { // Create projected schema
 * List<org.apache.arrow.vector.types.pojo.Field> projectedFields =
 * projectedColumns .stream().map( name -> arrowSchema.getFields().stream()
 * .filter(f -> f.getName().equals(name)).findFirst() .orElseThrow(() -> new
 * IllegalArgumentException( "Column not found: " + name)))
 * .collect(Collectors.toList());
 *
 * this.projectedArrowSchema = new org.apache.arrow.vector.types.pojo.Schema(
 * projectedFields); }
 *
 * }
 *
 * @Override public boolean hasNext() { batch = nextBatch(); return batch !=
 * null; }
 *
 * @Override public RecordBatch next() { if (batch == null) { throw new
 * NoSuchElementException(); } RecordBatch result = batch;
 * System.out.println("ParquetIterator.next: " + result.toCsv()); batch = null;
 * return result; }
 *
 * private RecordBatch nextBatch() { try (PageReadStore pages =
 * reader.readNextRowGroup()) { if (pages == null) {
 * System.out.println("No more pages"); return null; }
 *
 * if (pages.getRowCount() > Integer.MAX_VALUE) { throw new
 * IllegalStateException("Row count exceeds maximum integer value"); }
 *
 * int rows = (int) pages.getRowCount();
 *
 * VectorSchemaRoot root = VectorSchemaRoot.create(projectedArrowSchema, new
 * RootAllocator(Long.MAX_VALUE)); root.allocateNew(); root.setRowCount(rows);
 *
 * Schema convertedSchema = SchemaConverter.fromArrow(projectedArrowSchema);
 *
 * return new RecordBatch(convertedSchema, root.getFieldVectors().stream()
 * .map(ArrowFieldVector::new).collect(Collectors.toList())); } catch
 * (IOException e) { // TODO Auto-generated catch block e.printStackTrace(); }
 * return batch; } }
 */