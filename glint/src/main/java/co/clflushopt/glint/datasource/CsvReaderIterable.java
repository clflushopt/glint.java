package co.clflushopt.glint.datasource;

import java.util.Iterator;

import com.univocity.parsers.csv.CsvParser;

import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class CsvReaderIterable implements Iterable<RecordBatch> {
    private final Schema schema;
    private final CsvParser parser;
    private final int batchSize;

    public CsvReaderIterable(Schema schema, CsvParser parser, int batchSize) {
        this.schema = schema;
        this.parser = parser;
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<RecordBatch> iterator() {
        return new CsvReaderIterator(schema, parser, batchSize);
    }

}
