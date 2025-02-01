package co.clflushopt.glint;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.core.CsvReaderOptions;
import co.clflushopt.glint.core.ExecutionContext;
import co.clflushopt.glint.dataframe.DataFrame;
import co.clflushopt.glint.query.logical.expr.AggregateExpr;
import co.clflushopt.glint.query.logical.expr.CastExpr;
import co.clflushopt.glint.query.logical.expr.ColumnExpr;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.optimizer.QueryOptimizer;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.Field;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Welcome to the Glint query compiler");
        try {
            nycTripsBenchmark(args);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void nycTripsBenchmark(String[] args) throws FileNotFoundException {
        // Create execution context
        ExecutionContext ctx = ExecutionContext.create().build();

        long startTime = System.currentTimeMillis();
        try {
            // Define the schema for NYC Taxi dataset
            Schema schema = new Schema(Arrays.asList(new Field("VendorID", ArrowTypes.Int32Type),
                    new Field("tpep_pickup_datetime", ArrowTypes.StringType), // Could be Timestamp
                    new Field("tpep_dropoff_datetime", ArrowTypes.StringType), // Could be Timestamp
                    new Field("passenger_count", ArrowTypes.Int32Type),
                    new Field("trip_distance", ArrowTypes.DoubleType),
                    new Field("pickup_longitude", ArrowTypes.DoubleType),
                    new Field("pickup_latitude", ArrowTypes.DoubleType),
                    new Field("RatecodeID", ArrowTypes.Int32Type),
                    new Field("store_and_fwd_flag", ArrowTypes.StringType),
                    new Field("dropoff_longitude", ArrowTypes.DoubleType),
                    new Field("dropoff_latitude", ArrowTypes.DoubleType),
                    new Field("payment_type", ArrowTypes.Int32Type),
                    new Field("fare_amount", ArrowTypes.DoubleType),
                    new Field("extra", ArrowTypes.DoubleType),
                    new Field("mta_tax", ArrowTypes.DoubleType),
                    new Field("tip_amount", ArrowTypes.DoubleType),
                    new Field("tolls_amount", ArrowTypes.DoubleType),
                    new Field("improvement_surcharge", ArrowTypes.DoubleType),
                    new Field("total_amount", ArrowTypes.DoubleType)));
            // Create DataFrame and apply transformations
            DataFrame df = ctx
                    .readCsv("./datasets/yellow_tripdata_example.csv", Optional.of(schema),
                            CsvReaderOptions.builder().delimiter(',').hasHeader(true).build())
                    .aggregate(List.of(col("passenger_count")),
                            List.of(max(cast(col("fare_amount"), ArrowTypes.FloatType))));

            System.out.println("Logical Plan:\t" + LogicalPlan.format(df.getLogicalPlan()));
            System.out.println("Schema:\t" + df.getSchema());

            // Optimize and execute the plan
            LogicalPlan optimizedPlan = QueryOptimizer.optimize(df.getLogicalPlan());
            System.out.println("Optimized Plan:\t" + LogicalPlan.format(optimizedPlan));

            // Execute and print results
            Iterator<RecordBatch> results = ctx.execute(optimizedPlan);

            while (results.hasNext()) {
                RecordBatch batch = results.next();
                System.out.println(batch.getSchema());
                System.out.println(batch.toCsv());

            }

        } finally {
            long endTime = System.currentTimeMillis();
            System.out.println("Query took " + (endTime - startTime) + " ms");
        }
    }

    // Helper methods for creating expressions
    private static LogicalExpr col(String name) {
        return new ColumnExpr(name);
    }

    private static LogicalExpr cast(LogicalExpr expr, ArrowType targetType) {
        return new CastExpr(expr, targetType);
    }

    private static AggregateExpr max(LogicalExpr expr) {
        return new AggregateExpr.Max(expr);
    }
}