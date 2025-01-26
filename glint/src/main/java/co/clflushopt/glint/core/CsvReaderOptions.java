package co.clflushopt.glint.core;

/**
 * Configuration options for reading CSV files, focusing on the most common
 * settings. Uses the builder pattern for a clean, type-safe configuration API.
 *
 */
public class CsvReaderOptions {
    private final boolean hasHeader;
    private final char delimiter;
    private final String tableName;

    private CsvReaderOptions(Builder builder) {
        this.hasHeader = builder.hasHeader;
        this.delimiter = builder.delimiter;
        this.tableName = builder.tableName;
    }

    public boolean hasHeader() {
        return hasHeader;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public String getTableName() {
        return tableName;
    }

    public static class Builder {
        private boolean hasHeader = true; // Default to true as it's common
        private char delimiter = ','; // Default to comma-separated
        private String tableName = "csv_scan";

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder hasHeader(boolean hasHeader) {
            this.hasHeader = hasHeader;
            return this;
        }

        public Builder delimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public CsvReaderOptions build() {
            return new CsvReaderOptions(this);
        }
    }

    /**
     * Creates a new builder for configuring CSV reading options.
     */
    public static Builder builder() {
        return new Builder();
    }
}
