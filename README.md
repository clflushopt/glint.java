# Glint: Vectorized and Code Generation Driven Query Engine in Java

> Briefly flashing the powers of query compilation without the machinery of a spark.

## Description

Glint is a minimal SQL query engine with vectorized and query compilation support in Java.

Following in the tradition of the new movement of modular database architectures
Glint has no catalog or data management; its only capability is turning SQL queries
into Java code that is then compiled and executed; think Calcite not Spark.

In order to make it fun, at least for tests and benchmark purposes, we did plug
an Arrow compatible API with support for Memory, CSV and Parquet data sources.

## Architecture

Architecting query compilers is a complicated and active field of research; in fact
Glint's architecture itself is inspired from several papers where the modularity
aspect of a query compiler is studied or demonstrated.

But before all of this, let's start with a brief tour of query engines in general this
will allow us to frame the architecture discussion in a concrete context by understanding
the fundamental components and patterns that shape modern query processing systems.

### Query Engine Architecture and Paradigms

Modern query engines traditionally follow the Volcano/Iterator model, where each operator
pulls data from its children one tuple at a time. If you want ton see what a simple
engine looks like see my other project [eocene](https://github.com/clflushopt/eocene).

```sh

SELECT col1 FROM table WHERE col2 > 10

            Projection(col1)
                  ↑
            Filter(col2 > 10)
                  ↑
              TableScan
```

Driving the execution of the above model are two execution paradigms: vectorized and compiled.
Vectorized execution processes data in batches (vectors) to better utilize CPU caches and
enable SIMD operations.

Instead of processing one row at a time like the Volcano model, it handles chunks of data

```
Data Chunk (e.g., 1024 rows)
    ┌─────────────────────┐
    │ col1 │ col2 │ col3  │  ──► Operator1 ──► Operator2 ──► ...
    │ ...  │ ...  │ ...   │     (processes   (processes
    └─────────────────────┘      vectors)      vectors)
```

Compiled execution, which our engine uses, takes a different approach by generating specialized
code for each query. Instead of interpreting a query plan, it produces native code that directly
implements the query logic.

```sh
SQL Query ──► Query Plan ──► Code Generation ──► Compiled Code
                                                    │
   Example compiled loop:                           ↓
   for(row in table) {                        Native Execution
     if(row.col2 > 10) {                      (No interpretation
       result.add(row.col1);                   overhead)
     }
   }
```

This has the benefit of eliminating a lot of the interpretation overhead at the cost of code
complexity.

Each approach has its trade-offs: Vectorized engines have lower compilation overhead and are
more flexible for dynamic workloads, while compiled engines can achieve better absolute performance
for stable queries by generating specialized code paths.

In a paper by Timo Kersten and others - [Everything You Always Wanted to Know About Compiled and Vectorized Queries But Were Afraid to Ask](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) they showed that the performance of
both approaches was pretty much on-par, with the results showing that data-centric code generation
being slightly better at compute intensive queries and vectorized being better at memory-bound
queries.

### Implementation Details

```

┌─────────────────────────────────────────────────────────┐
│                    DataFrame API                        │
├─────────────────────────────────────────────────────────┤
│                  Logical Planning                       │
│  ┌─────────────┐   ┌──────────┐    ┌───────────────┐    │
│  │     Scan    │   │  Join    │    │   Project     │    │
│  └─────────────┘   └──────────┘    └───────────────┘    │
├─────────────────────────────────────────────────────────┤
│                 Physical Planning                       │
│  ┌─────────────┐   ┌─────────┐      ┌─────────────┐     │
│  │ TableScan   │   │HashJoin │      │Project      │     │
│  └─────────────┘   └─────────┘      └─────────────┘     │
├─────────────────────────────────────────────────────────┤
│                    Execution                            │
│  ┌─────────────┐   ┌─────────┐      ┌─────────────┐     │
│  │ScanOperator │   │ JoinOp  │      │  ProjectOp  │     │
│  └─────────────┘   └─────────┘      └─────────────┘     │
└─────────────────────────────────────────────────────────┘
           │              │                │
           └──────────────┼────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────┐
│                 Apache Arrow                            │
└─────────────────────────────────────────────────────────┘

```

- Apache Arrow Integration:
  - Uses Arrow's columnar memory format throughout the engine
  - Leverages Arrow's VectorSchemaRoot for batch processing
  - Implements custom FieldVector wrappers for type safety
  - Enables zero-copy data sharing between operations

- Three-Layer Architecture:
  - Logical Plans: Abstract representation of operations (WHAT)
  - Physical Plans: Concrete implementation strategies (HOW)
  - Operators: Actual execution code using Volcano model

- DataFrame API:
  - Provides a fluent interface for query construction
  - Supports common operations (select, filter, join)
  - Handles schema inference and validation
  - Abstracts query planning complexity from users

- Query Execution:
  - Uses vectorizewd Volcano-style iterator model
  - Processes data in batches for efficiency
  - Supports push-down optimizations
  - Implements memory-efficient operations

  ### Running the examples

You will probably want to use an IDE like IntelliJ or what I personally recommend VSCode with the
Java pack at least for working with the codebase but you are free to use ed or nano as well.

Running this thing will require Maven for no other reason than trying to run it without Maven
has made me realize this will be the last and only time I write Java as a hobby or professionaly.

If you don't want Maven; you should be able to figure it out.

```sh

$ export JDK_JAVA_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
$ export MAVEN_OPTS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"

$ mvn compile exec:java --file glint/pom.xml
```

```
Schema [fields=[(name: passenger_count, type: Int(32, true)), (name: MAX, type: FloatingPoint(SINGLE))]]

Logical Plan:   Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(CAST(#fare_amount AS FLOAT))]
    Scan: parquet_scan [projection=None]
Optimized Plan: Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(CAST(#fare_amount AS FLOAT))]
      Scan: parquet_scan [projection=None]

Results:

0,36090.3
1,623259.9
2,492.5
3,350.0
4,500.0
5,760.0
6,262.5
7,78.0
8,87.0
9,92.0
null,103.2

Query took 2758 ms
```