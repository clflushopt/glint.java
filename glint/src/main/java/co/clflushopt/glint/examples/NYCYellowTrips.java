package co.clflushopt.glint.examples;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.ArrowType;

import co.clflushopt.glint.core.CsvReaderOptions;
import co.clflushopt.glint.core.DatasetUtils;
import co.clflushopt.glint.core.ExecutionContext;
import co.clflushopt.glint.dataframe.DataFrame;
import co.clflushopt.glint.query.logical.expr.LogicalAggregateExpr;
import co.clflushopt.glint.query.logical.expr.LogicalCastExpr;
import co.clflushopt.glint.query.logical.expr.LogicalColumnExpr;
import co.clflushopt.glint.query.logical.expr.LogicalExpr;
import co.clflushopt.glint.query.logical.plan.LogicalPlan;
import co.clflushopt.glint.query.optimizer.QueryOptimizer;
import co.clflushopt.glint.types.ArrowTypes;
import co.clflushopt.glint.types.RecordBatch;
import co.clflushopt.glint.types.Schema;

public class NYCYellowTrips {
    public static void runCSVExample() throws FileNotFoundException {
        // Create execution context
        ExecutionContext ctx = ExecutionContext.create().build();

        long startTime = System.currentTimeMillis();
        try {
            // Define the schema for NYC Taxi dataset
            Schema schema = DatasetUtils.getNYCYellowTripsCSVSchema();
            // Create DataFrame and apply transformations
            DataFrame df = ctx
                    .readCsv("./datasets/yellow_tripdata_2019-01.csv", Optional.of(schema),
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

    public static void runParquetExample() throws FileNotFoundException {
        // Create execution context
        ExecutionContext ctx = ExecutionContext.create().build();

        long startTime = System.currentTimeMillis();
        try {
            Path path = Path.of("datasets/yellow_tripdata_2019-01.parquet");
            // Define the schema for NYC Taxi dataset
            // Create DataFrame and apply transformations
            DataFrame df = ctx
                    .readParquet(path.toAbsolutePath().toString(),
                            Optional.of(DatasetUtils.getNYCYellowTripsCSVSchema()))
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
        return new LogicalColumnExpr(name);
    }

    private static LogicalExpr cast(LogicalExpr expr, ArrowType targetType) {
        return new LogicalCastExpr(expr, targetType);
    }

    private static LogicalAggregateExpr max(LogicalExpr expr) {
        return new LogicalAggregateExpr.Max(expr);
    }

}
