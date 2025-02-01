package co.clflushopt.glint.types;

import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class SchemaConverter {
    public static Schema fromArrow(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
        var fields = arrowSchema.getFields().stream()
                .map(field -> new Field(field.getName(), field.getType()))
                .collect(Collectors.toList());
        return new Schema(fields);
    }

    public static Schema fromParquet(org.apache.parquet.schema.MessageType parquetSchema) {
        var fields = parquetSchema.getFields().stream()
                .map(field -> new Field(field.getName(), toArrowType(field)))
                .collect(Collectors.toList());
        return new Schema(fields);
    }

    public static ArrowType toArrowType(org.apache.parquet.schema.Type parquetType) {
        switch (parquetType.getRepetition()) {
        case REQUIRED:
            break;
        case OPTIONAL:
            break;
        case REPEATED:
            break;
        default:
            break;
        }

        switch (parquetType.asPrimitiveType().getPrimitiveTypeName()) {
        case BOOLEAN:
            return ArrowTypes.BooleanType;
        case INT32:
            return ArrowTypes.Int32Type;
        case INT64:
            return ArrowTypes.Int64Type;
        case FLOAT:
            return ArrowTypes.FloatType;
        case DOUBLE:
            return ArrowTypes.DoubleType;
        case BINARY:
            return ArrowTypes.StringType;
        default:
            throw new IllegalArgumentException("Unsupported type: " + parquetType);
        }
    }
}
