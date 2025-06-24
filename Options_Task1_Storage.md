# Task 1 & Task 2: Exploration and Evaluation of Storage Solutions for Large Genomic Datasets

**Prepared by:** Abdu Mohammed  
**Date:** June 24, 2025

---

## Task 1: Explore Storage and Query Options for Large Genomic Datasets

### Objective
Identify scalable and efficient storage and querying methods suitable for large genomic datasets characterized by complex relationships and high volume.

### Solutions Explored

| Storage Solution | Type          | Description                                              |
|------------------|---------------|----------------------------------------------------------|
| Neo4j            | Native Graph DB| Industry-standard graph database with ACID compliance; stores nodes, edges, and properties natively. |
| TigerGraph       | Native Graph DB| High-performance graph DB optimized for large-scale parallel graph analytics. |
| MongoDB          | Document DB   | NoSQL document-oriented database with flexible JSON-like schemas, considered for offloading node properties. |
| DuckDB + Parquet | Columnar OLAP | Analytical DB optimized for fast, compressed queries on tabular data; paired with Parquet file format for efficient storage. |

### Exploration Summary

- **Neo4j** is the existing graph database platform, providing strong native graph capabilities and good performance for connected data.
- **TigerGraph** demonstrated superior raw graph query speed but is limited by enterprise licensing constraints, which restrict free use in our environment.
- Offloading node and edge **properties** from Neo4j to an external store like **MongoDB** was considered to reduce Neo4j’s storage burden and improve scalability.
- **DuckDB with Parquet** was explored as an analytical backend for large disconnected property datasets to benefit from compression and high-speed query performance.
- Hybrid architectures combining Neo4j (graph structure) with external property stores were conceptualized to balance performance and scalability.

---

## Task 2: Evaluation of Storage Solutions for Scalability and Performance

### Evaluation Criteria

| Criteria        | Description                                      |
|-----------------|--------------------------------------------------|
| Performance     | Query speed and throughput on representative genomic queries. |
| Scalability     | Ability to handle growing dataset sizes efficiently. |
| Cost & Licensing| Availability of free or enterprise licenses and associated costs. |
| Integration     | Ease of integrating with existing infrastructure and workflows. |
| Storage Efficiency| Storage size and data compression capabilities. |

### Evaluation Summary

| Storage Solution | Performance                       | Scalability                     | Cost & Licensing          | Integration                 | Storage Efficiency         | Notes                              |
|------------------|---------------------------------|--------------------------------|--------------------------|-----------------------------|----------------------------|-----------------------------------|
| Neo4j            | Good for graph traversals        | Limited by graph size           | Free community edition   | Already in use               | Moderate                   | Baseline system                   |
| TigerGraph       | Superior speed on complex queries| High (massively parallel)       | Enterprise license needed| Limited free use             | Good                       | Not feasible due to cost/licensing|
| MongoDB          | Moderate; impacted by API overhead| Scalable for property data      | Free/community edition   | Requires API integration     | Good                       | Introduces query latency         |
| DuckDB + Parquet | High for batch analytical queries| Excellent for large datasets    | Open source              | Standalone, integrates via files| Excellent compression     | Not suitable for graph queries   |

---

### Conclusions and Recommendations

- **Neo4j remains the best choice for storing and querying connected graph data** due to its mature ecosystem and existing deployment.
- **TigerGraph’s speed advantage is outweighed by licensing limitations**, making it impractical for this project.
- **Using MongoDB as a separate property store offers scalability and schema flexibility but adds API call overhead that reduces query speed**.
- **DuckDB with Parquet files is excellent for handling large disconnected property datasets**, offering efficient storage and fast analytics.
- A **hybrid approach**, combining Neo4j for graph structure and DuckDB/MongoDB for properties, balances performance, scalability, and cost.
- Further performance tuning and caching strategies are required to mitigate latency introduced by cross-database querying in the hybrid model.

---

### Deliverables

- Comparison tables (this document and spreadsheet)
- Summary report (this markdown file)
- Optional video walkthrough (to be recorded)

---

**End of Task 1 & 2 Deliverable**
