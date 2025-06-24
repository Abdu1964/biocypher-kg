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

➡️ [Commit: MongoDB Writer](https://github.com/Abdu1964/biocypher-kg/commit/c89278a40c437da9c414e0b56f555e9ff7034b0c)

---

#### ✅ 2. `knowledge_graph.py` (Pipeline Orchestrator)
- Coordinates the entire process:
  - Loads adapter configurations
  - Preprocesses schemas
  - Initializes the writer
  - Triggers writing to MongoDB

➡️ [Commit: BioCypher KG Pipeline](https://github.com/Abdu1964/biocypher-kg/commit/b3a89bceed9f27475fb5b2b392fb3ce695b6a467)

---

#### ✅ 3. `Flask API`
- Serves as a middleware to expose MongoDB through a RESTful interface.
- Improves data access control and simplifies querying for downstream applications.

➡️ [Commit: Flask API](https://github.com/Abdu1964/biocypher-kg/commit/2e8a5f1e3c9552137a9cb7ca0f923d7fb1732f30)

---

#### ✅ 4. `FastAPI` Version (Enhanced Interface)
- Alternative API implementation with FastAPI for better performance and async handling.
- Uses Neo4j’s APOC to query MongoDB directly based on node IDs stored in Neo4j.

➡️ [Commit: FastAPI Integration](https://github.com/Abdu1964/biocypher-kg/commit/4f22d029e2ab2c003f76d8d50f0ba8547e0f4c92)

---

## 📊 Results and Observations

- ✅ **Storage**: Using MongoDB for properties allows scalable and flexible property storage, ideal for rich biomedical or genomic datasets.
- ⚠️ **Performance**:
  - Querying from MongoDB via API introduces **network overhead** and **API round trips**.
  - Performance is acceptable for **small datasets**, but **very slow for large-scale queries** due to MongoDB’s response time and the overhead of cross-service querying.
- ✅ **Neo4j Traversal**: Graph queries remain very fast, since only essential IDs are stored.

## ✅ Conclusion

This experiment directly addressed the two assigned tasks:

### **1. Implement a separate property database to optimize querying speed**

- ✅ A **MongoDB-based property database** was successfully implemented to store rich node and edge properties.
- ✅ **Neo4j was used to store only node IDs and relationships**, significantly reducing graph database size and improving traversal speed.
- ⚠️ While MongoDB offers excellent flexibility and storage for complex data, using it **as a live query backend (via API or APOC)** introduced noticeable latency.
-I have also attached a video demonstrating the implementation of a separate property database and the benchmarking of query performance with and without property-based storage [Video:separate property database to optimize querying speed](https://drive.google.com/drive/folders/1T0aMPfy9cuqJYceSLK4v7jDAUdRHaHPx)

---

### **2. Benchmark and compare query performance with and without property-based storage**

- 🧪 **Benchmark Setup**:
  - Tested with all data (properties + structure) stored directly in Neo4j.
  - Then tested with a split setup: **graph structure in Neo4j**, **properties in MongoDB**.
- 📉 **Findings**:
  - Neo4j-only setup provides **faster property access**, but at the cost of increased storage and complexity inside the graph DB.
  - The MongoDB + Neo4j setup **reduced storage in Neo4j**, but **introduced performance overhead** due to:
    - API round trips (via Flask/FastAPI)
    - Cross-database coordination
    - Slower batch query responses when accessing large property sets
- ⚠️ Even with batch processing, the **querying delay from MongoDB** is significant compared to native Neo4j queries.
- I have also attached a video demonstrating the implementation of a separate property database and the benchmarking of query performance with and without property-based storage [Video:Benchmark and compare query performance with and without property-based storage](https://drive.google.com/drive/folders/1T0aMPfy9cuqJYceSLK4v7jDAUdRHaHPx)

---

### 🧠 Final Takeaway

> While separating the property database improves modularity and reduces graph database bloat, it comes at the **cost of query performance** — especially for **large-scale property retrieval**. A hybrid model is useful for storage optimization and architectural flexibility, but **not ideal for high-speed querying**.

---

## 📌 Repository

🔗 [View the full project on GitHub](https://github.com/Abdu1964/biocypher-kg/commits/Storage_Query_Optimization/)
