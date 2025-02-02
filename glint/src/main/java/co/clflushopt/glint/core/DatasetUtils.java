package co.clflushopt.glint.core;

import java.util.Arrays;

import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.Schema;

public class DatasetUtils {

    public static final Schema getNYCYellowTripsCSVSchema() {
        return new Schema(Arrays.asList(new Field("VendorID", ArrowTypes.Int32Type),
                new Field("tpep_pickup_datetime", ArrowTypes.StringType),
                new Field("tpep_dropoff_datetime", ArrowTypes.StringType),
                new Field("passenger_count", ArrowTypes.Int32Type),
                new Field("trip_distance", ArrowTypes.DoubleType),
                new Field("RatecodeID", ArrowTypes.Int32Type),
                new Field("store_and_fwd_flag", ArrowTypes.StringType),
                new Field("PULocationID", ArrowTypes.Int32Type),
                new Field("DOLocationID", ArrowTypes.Int32Type),
                new Field("payment_type", ArrowTypes.Int32Type),
                new Field("fare_amount", ArrowTypes.DoubleType),
                new Field("extra", ArrowTypes.DoubleType),
                new Field("mta_tax", ArrowTypes.DoubleType),
                new Field("tip_amount", ArrowTypes.DoubleType),
                new Field("tolls_amount", ArrowTypes.DoubleType),
                new Field("improvement_surcharge", ArrowTypes.DoubleType),
                new Field("total_amount", ArrowTypes.DoubleType)));
    }

}
