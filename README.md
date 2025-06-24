# 🚀 Knowledge Graph Property Database Experiment

This repository presents an experimental implementation of a **separate property database** architecture for knowledge graphs using **MongoDB** and **Neo4j**, focused on optimizing query performance and scalability.

---

## 🎯 Objective

The goals of the experiment were to:

1. **Implement a separate property database** to optimize querying speed.
2. **Benchmark and compare query performance** with and without property-based storage.

---

## ⚙️ What Was Implemented

To achieve this, I designed a system that separates the **graph structure** and **property data** using the following approach:

### 🔁 Dual Storage Design

- **Neo4j** stores only lightweight **node IDs and relationships** to ensure high-speed graph traversals and path queries.
- **MongoDB** serves as the **property database**, storing **all detailed properties of nodes and edges** in a flexible JSON-like format.

This architecture reduces storage overhead in Neo4j and leverages MongoDB’s scalability and schema flexibility for complex property data.

---

### 🧱 Components

#### ✅ 1. `MongodbWriter`
- Writes nodes and edges' properties to MongoDB collections.
- Supports structured and flexible property writing via custom adapters.

➡️ [Commit: MongoDB Writer](https://github.com/Abdu1964/Abdu1964-Storage_Query_Optimization/commit/c89278a)

---

#### ✅ 2. `knowledge_graph.py` (Pipeline Orchestrator)
- Coordinates the entire process:
  - Loads adapter configurations
  - Preprocesses schemas
  - Initializes the writer
  - Triggers writing to MongoDB

➡️ [Commit: BioCypher KG Pipeline](https://github.com/Abdu1964/Abdu1964-Storage_Query_Optimization/commit/b3a89bc)

---

#### ✅ 3. `Flask API`
- Serves as a middleware to expose MongoDB through a RESTful interface.
- Improves data access control and simplifies querying for downstream applications.

➡️ [Commit: Flask API](https://github.com/Abdu1964/Abdu1964-Storage_Query_Optimization/commit/2e8a5f1)

---

#### ✅ 4. `FastAPI` Version (Enhanced Interface)
- Alternative API implementation with FastAPI for better performance and async handling.
- Uses Neo4j’s APOC to query MongoDB directly based on node IDs stored in Neo4j.

➡️ [Commit: FastAPI Integration](https://github.com/Abdu1964/Abdu1964-Storage_Query_Optimization/commit/4f22d02)

---

## 📊 Results and Observations

- ✅ **Storage**: Using MongoDB for properties allows scalable and flexible property storage, ideal for rich biomedical or genomic datasets.
- ⚠️ **Performance**:
  - Querying from MongoDB via API introduces **network overhead** and **API round trips**.
  - Performance is acceptable for **small datasets**, but **very slow for large-scale queries** due to MongoDB’s response time and the overhead of cross-service querying.
- ✅ **Neo4j Traversal**: Graph queries remain very fast, since only essential IDs are stored.

---

## 🧠 Summary

While MongoDB is well-suited for storing rich property data due to its document model, using it as a live query backend (especially through API and APOC) introduces latency and bottlenecks. For optimal performance:
- Neo4j should handle core graph logic.
- MongoDB can serve **enrichment** or **external property resolution**, not intensive real-time queries.

---

## 📌 Repository

🔗 [View the full project on GitHub](https://github.com/Abdu1964/Abdu1964-Storage_Query_Optimization)
