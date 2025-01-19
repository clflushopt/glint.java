package co.clflushopt.glint.datasource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;

import co.clflushopt.glint.types.ArrowFieldVector;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;
import co.clflushopt.glint.util.IndexedStream;

public class CsvReaderIterator implements Iterator<RecordBatch> {
    private final Schema schema;
    private final CsvParser parser;
    private final int batchSize;
    private RecordBatch next;
    private boolean started;

    public CsvReaderIterator(Schema schema, CsvParser parser, int batchSize) {
        this.schema = schema;
        this.parser = parser;
        this.batchSize = batchSize;
        this.started = false;
    }

    @Override
    public boolean hasNext() {
        if (!started) {
            started = true;
            next = nextBatch();
        }
        return next != null;
    }

    @Override
    public RecordBatch next() {
        if (!started) {
            hasNext();
        }

        RecordBatch out = next;
        next = nextBatch();

        if (out == null) {
            throw new NoSuchElementException(
                    "Cannot read past the end of " + CsvReaderIterator.class.getSimpleName());
        }

        return out;
    }

    private RecordBatch nextBatch() {
        ArrayList<Record> rows = new ArrayList<>(batchSize);

        Record line;
        do {
            line = parser.parseNextRecord();
            if (line != null) {
                rows.add(line);
            }
        } while (line != null && rows.size() < batchSize);

        if (rows.isEmpty()) {
            return null;
        }

        return createBatch(rows);
    }

    private RecordBatch createBatch(ArrayList<Record> rows) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema.toArrow(),
                new RootAllocator(Long.MAX_VALUE));
        root.getFieldVectors().forEach(v -> v.setInitialCapacity(rows.size()));
        root.allocateNew();

        IndexedStream.withIndex(root.getFieldVectors()).forEach(field -> {
            ValueVector vector = field.getValue();
            if (vector instanceof VarCharVector) {
                VarCharVector varCharVector = (VarCharVector) vector;
                IndexedStream.withIndex(rows).forEach(row -> {
                    String valueStr = row.getValue().getValue(vector.getName(), "").trim();
                    varCharVector.setSafe(row.getIndex(), valueStr.getBytes());
                });
            } else if (vector instanceof TinyIntVector) {
                TinyIntVector tinyIntVector = (TinyIntVector) vector;
                IndexedStream.withIndex(rows).forEach(row -> {
                    String valueStr = row.getValue().getValue(vector.getName(), "").trim();
                    if (valueStr.isEmpty()) {
                        tinyIntVector.setNull(row.getIndex());
                    } else {
                        tinyIntVector.set(row.getIndex(), Byte.parseByte(valueStr));
                    }
                });
            } else if (vector instanceof SmallIntVector) {
                SmallIntVector smallIntVector = (SmallIntVector) vector;
                IndexedStream.withIndex(rows).forEach(row -> {
                    String valueStr = row.getValue().getValue(vector.getName(), "").trim();
                    if (valueStr.isEmpty()) {
                        smallIntVector.setNull(row.getIndex());
                    } else {
                        smallIntVector.set(row.getIndex(), Short.parseShort(valueStr));
                    }
                });
            } else if (vector instanceof IntVector) {
                IntVector intVector = (IntVector) vector;
                IndexedStream.withIndex(rows).forEach(row -> {
                    String valueStr = row.getValue().getValue(vector.getName(), "").trim();
                    if (valueStr.isEmpty()) {
                        intVector.setNull(row.getIndex());
                    } else {
                        intVector.set(row.getIndex(), Integer.parseInt(valueStr));
                    }
                });
            } else if (vector instanceof BigIntVector) {
                BigIntVector bigIntVector = (BigIntVector) vector;
                IndexedStream.withIndex(rows).forEach(row -> {
                    String valueStr = row.getValue().getValue(vector.getName(), "").trim();
                    if (valueStr.isEmpty()) {
                        bigIntVector.setNull(row.getIndex());
                    } else {
                        bigIntVector.set(row.getIndex(), Long.parseLong(valueStr));
                    }
                });
            } else if (vector instanceof Float4Vector) {
                Float4Vector float4Vector = (Float4Vector) vector;
                IndexedStream.withIndex(rows).forEach(row -> {
                    String valueStr = row.getValue().getValue(vector.getName(), "").trim();
                    if (valueStr.isEmpty()) {
                        float4Vector.setNull(row.getIndex());
                    } else {
                        float4Vector.set(row.getIndex(), Float.parseFloat(valueStr));
                    }
                });
            } else if (vector instanceof Float8Vector) {
                Float8Vector float8Vector = (Float8Vector) vector;
                IndexedStream.withIndex(rows).forEach(row -> {
                    String valueStr = row.getValue().getValue(vector.getName(), "").trim();
                    if (valueStr.isEmpty()) {
                        float8Vector.setNull(row.getIndex());
                    } else {
                        float8Vector.set(row.getIndex(), Double.parseDouble(valueStr));
                    }
                });
            } else {
                throw new IllegalStateException(
                        "No support for reading CSV columns with data type " + vector);
            }
            vector.setValueCount(rows.size());
        });

        return new RecordBatch(schema, root.getFieldVectors().stream().map(ArrowFieldVector::new)
                .collect(Collectors.toList()));
    }
}
