# Pandas vs PySpark MovieLens Comparison — Execution Report

## Executive summary

The notebook `notebooks/03_pandas_vs_pyspark_comparison.ipynb` ran
successfully against the local MovieLens production files.

- The automatic data check found both CSV files, so no download was needed.
- Pandas used the intended memory-safe sample of 1,000,000 rating rows.
- PySpark processed the complete 33,832,162-row ratings file.
- Both approaches completed their aggregations, joins, top-10 ranking, and
  timing examples without errors.
- The timing results are not a direct performance contest because Pandas and
  PySpark processed different amounts of data.

## Execution context

| Item | Observed result |
|---|---|
| Project root | `/home/seans/becode/projects/movie-recommendation-pyspark` |
| Ratings input | `data/raw/ml-latest/ratings.csv` |
| Movies input | `data/raw/ml-latest/movies.csv` |
| Ratings file size | 890.6 MiB |
| Dataset provisioning | Files already existed; download skipped |
| Notebook result | Completed successfully |

## Pandas results

### Data loaded

Because the ratings CSV exceeded the notebook's 250 MiB safety threshold,
Pandas loaded the first 1,000,000 rows rather than the full file.

| DataFrame | Shape | Columns | In-memory size |
|---|---:|---|---:|
| Ratings sample | 1,000,000 × 4 | `userId`, `movieId`, `rating`, `timestamp` | 30.5 MiB |
| Movies | 86,537 × 3 | `movieId`, `title`, `genres` | 5.2 MiB |

The sample represents approximately 2.96% of the full rating-row count. It is
the first million rows, not a random sample, so its statistics should be
treated as illustrative rather than population estimates.

### Pandas aggregation metrics

| Metric | Result |
|---|---:|
| Rating rows processed | 1,000,000 |
| Unique users | 9,561 |
| Unique movies rated | 25,825 |
| Average rating | 3.522055 |
| Minimum rating | 0.5 |
| Maximum rating | 5.0 |

### Pandas top 10 most-rated movies in the sample

| Rank | Movie | Rating count |
|---:|---|---:|
| 1 | The Shawshank Redemption (1994) | 3,550 |
| 2 | Forrest Gump (1994) | 3,326 |
| 3 | Pulp Fiction (1994) | 3,164 |
| 4 | The Matrix (1999) | 3,157 |
| 5 | The Silence of the Lambs (1991) | 3,011 |
| 6 | Star Wars: Episode IV — A New Hope (1977) | 2,858 |
| 7 | Fight Club (1999) | 2,497 |
| 8 | Schindler's List (1993) | 2,492 |
| 9 | Jurassic Park (1993) | 2,411 |
| 10 | The Lord of the Rings: The Fellowship of the Ring (2001) | 2,381 |

## PySpark results

### Inferred schemas

`ratings.csv`:

| Column | Spark type | Nullable |
|---|---|---|
| `userId` | integer | yes |
| `movieId` | integer | yes |
| `rating` | double | yes |
| `timestamp` | integer | yes |

`movies.csv`:

| Column | Spark type | Nullable |
|---|---|---|
| `movieId` | integer | yes |
| `title` | string | yes |
| `genres` | string | yes |

“Nullable” is Spark's inferred schema capability; it does not mean null values
were observed in these columns.

### Full-dataset PySpark aggregation metrics

| Metric | Result |
|---|---:|
| Rating rows processed | 33,832,162 |
| Movie metadata rows | 86,537 |
| Unique users | 330,975 |
| Unique movies rated | 83,239 |
| Average rating | 3.542540 |
| Minimum rating | 0.5 |
| Maximum rating | 5.0 |

### PySpark top 10 most-rated movies in the full dataset

| Rank | Movie | Rating count |
|---:|---|---:|
| 1 | The Shawshank Redemption (1994) | 122,296 |
| 2 | Forrest Gump (1994) | 113,581 |
| 3 | Pulp Fiction (1994) | 108,756 |
| 4 | The Matrix (1999) | 107,056 |
| 5 | The Silence of the Lambs (1991) | 101,802 |
| 6 | Star Wars: Episode IV — A New Hope (1977) | 97,202 |
| 7 | Fight Club (1999) | 86,207 |
| 8 | Schindler's List (1993) | 84,232 |
| 9 | Jurassic Park (1993) | 83,026 |
| 10 | Star Wars: Episode V — The Empire Strikes Back (1980) | 80,200 |

## Comparison and interpretation

### Metric scope

| Measure | Pandas | PySpark |
|---|---:|---:|
| Rows processed | 1,000,000 | 33,832,162 |
| Data scope | First-row sample | Full ratings CSV |
| Unique users | 9,561 | 330,975 |
| Unique movies rated | 25,825 | 83,239 |
| Average rating | 3.522055 | 3.542540 |
| Rating range | 0.5–5.0 | 0.5–5.0 |

The average ratings are close, differing by about 0.0205 points, but the
Pandas sample is ordered by source-file position and is not random. This
similarity should therefore not be presented as proof that the sample is fully
representative.

Nine of the first ten most-rated titles appear in the same rank positions in
both outputs. The tenth differs:

- Pandas sample: *The Lord of the Rings: The Fellowship of the Ring*.
- Full PySpark data: *The Empire Strikes Back*.

This illustrates how a bounded sample can preserve major popularity patterns
while still changing results near a ranking boundary.

## Timing results

| Tool | Data scope | Timed operation | Time |
|---|---|---|---:|
| Pandas | First 1,000,000 rows | Group by movie and calculate mean | 0.148993 s |
| PySpark | Full 33,832,162 rows | Group by movie, calculate mean, then count | 20.539128 s |

These timings are useful observations from this machine, but they are **not a
fair benchmark**:

1. PySpark processed about 33.8 times more rating rows.
2. The Spark expression included a `count()` action and distributed-job
   scheduling overhead.
3. Pandas operated on data already materialized in one process.
4. Spark's benefit is the ability to scale the same DataFrame logic across
   partitions, cores, and potentially multiple machines—not guaranteed lower
   latency for a small local operation.

The correct presentation conclusion is:

> Pandas was quicker for this bounded local sample, while PySpark safely
> processed the complete dataset with a scalable execution model. The run does
> not prove that either tool is universally faster.

## Runtime warnings

Spark emitted several non-fatal local-environment warnings:

- The machine hostname resolved to a loopback address, so Spark selected
  another local interface. `SPARK_LOCAL_IP=127.0.0.1` can be set if binding
  problems occur.
- The native Hadoop library was unavailable, so Spark used built-in Java
  classes. This is common for a local CSV demonstration.
- Spark UI port `4040` was already occupied, so Spark used `4041`.

None of these warnings prevented execution or changed the reported results.

## Final findings

1. The memory guard worked as intended and prevented Pandas from attempting to
   materialize the 890.6 MiB CSV in full.
2. Pandas provided compact syntax and fast interactive processing for the
   one-million-row sample.
3. PySpark processed all 33.8 million ratings and returned full-dataset
   aggregates and rankings.
4. Both tools expressed the same logical workflow: load, aggregate, group,
   join, sort, and display.
5. The results support the project's design choice: use PySpark for large-scale
   rating processing and Pandas for small outputs or bounded exploration.

## Presentation-ready conclusion

Pandas and PySpark solve similar tabular problems but target different working
conditions. Pandas is concise and effective when data fits comfortably in one
machine's memory. PySpark adds startup and scheduling overhead, but its lazy,
partitioned execution model can process much larger datasets and carry the same
logic into a distributed environment. In this run, Pandas explored a safe
one-million-row sample, while PySpark processed the complete 33.8-million-row
MovieLens snapshot.
