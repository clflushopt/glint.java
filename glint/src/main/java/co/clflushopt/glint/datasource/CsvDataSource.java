package co.clflushopt.glint.datasource;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import com.google.common.collect.Streams;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

/**
 * CSV data source.
 */
public class CsvDataSource {
    private final Schema schema;
    private final String filename;
    private final Boolean hasHeaders;
    private final Integer batchSize;

    private final Logger logger = Logger.getLogger(CsvDataSource.class.getName());

    public CsvDataSource(String filename, Optional<Schema> schema, Boolean hasHeaders, Integer batchSize)
            throws FileNotFoundException {
        this.filename = filename;
        this.hasHeaders = hasHeaders;
        this.batchSize = batchSize;
        if (schema.isPresent()) {
            logger.info("Schema was provided " + schema.get().toString());
            this.schema = schema.get();
        } else {
            logger.info("No schema was provided, using inferred schema");
            this.schema = inferSchema();
        }
    }

    private Schema inferSchema() throws FileNotFoundException {
        logger.info("Schema inference triggered");

        var file = new File(filename);
        if (!file.exists()) {
            logger.info("File was not found");
            throw new FileNotFoundException("file with name " + filename + " was not found");
        }

        var parser = getCsvParser(getCsvDefaultSettings());
        parser.beginParsing(file);

        var format = parser.getDetectedFormat();
        logger.info(String.format("Detected format with delimiter: %s and line separator: %s",
                format.getDelimiterString(), format.getLineSeparator()));

        // Parse the next line which either is a header or a row sample.
        parser.parseNext();
        var headers = parser.getContext().parsedHeaders();

        // Stop parsing since the rest can be inferred from the headers we collected.
        parser.stopParsing();

        if (hasHeaders) {
            return new Schema(
                    List.of(headers).stream().map(columnName -> new Field(columnName, ArrowTypes.StringType)).toList());
        } else {
            return new Schema(Streams
                    .mapWithIndex(List.of(headers).stream(),
                            (_field, index) -> new Field(String.format("field_%d", index), ArrowTypes.StringType))
                    .toList());
        }

    }

    private CsvParserSettings getCsvDefaultSettings() {
        var defaultSettings = new CsvParserSettings();
        defaultSettings.setDelimiterDetectionEnabled(true);
        defaultSettings.setLineSeparatorDetectionEnabled(true);
        defaultSettings.setSkipEmptyLines(true);
        defaultSettings.setAutoClosingEnabled(true);
        defaultSettings.setHeaderExtractionEnabled(true);

        return defaultSettings;
    }

    private CsvParser getCsvParser(CsvParserSettings settings) {
        return new CsvParser(settings);
    }

    public Schema getSchema() {
        return schema;
    }

    public String getFilename() {
        return filename;
    }

    public Boolean getHasHeaders() {
        return hasHeaders;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Logger getLogger() {
        return logger;
    }

}
