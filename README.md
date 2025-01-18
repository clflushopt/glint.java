# Glint: SQL Query Compiler for Java

> Briefly flashing the powers of query compilation without the machinery of a spark.

## Description

Glint is a SQL query compiler capable of parsing, optimizing and compiling
SQL queries into Java bytecode.

Following in the tradition of the new movement of modular database architectures
Glint has no catalog or data management; its only capability is turning SQL queries
into Java code that is then compiled and executed; think Calcite not Spark.

In order to make it usable, at least for tests and benchmark purposes, we did plug
an Arrow compatible access API this way we can read parquet files that represent
virtual tables and execute our queries againt them.

## Architecture

Architecting query compilers is a complicated and active field of research; in fact
Glint's architecture itself is inspired by several recent papers where the modularity
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