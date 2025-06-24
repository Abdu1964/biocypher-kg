from fastapi import FastAPI, Query, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
import urllib.parse
from fastapi.middleware.gzip import GZipMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis
import os
from typing import Dict, List, Any

load_dotenv()

app = FastAPI(title="MongoDB Property Service for Neo4j")

# Add GZip compression for large property payloads
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Get MongoDB credentials from environment
username = urllib.parse.quote_plus(os.getenv("MONGODB_USERNAME"))
password = urllib.parse.quote_plus(os.getenv("MONGODB_PASSWORD"))
cluster = os.getenv("MONGODB_CLUSTER")
appname = os.getenv("MONGODB_APPNAME")

# Construct the URI
uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName={appname}"

# Async MongoDB client with connection pooling
mongo_client = AsyncIOMotorClient(uri, maxPoolSize=100, minPoolSize=10)
db = mongo_client["output_wzp"]

# Cache setup
@app.on_event("startup")
async def startup():
    redis = aioredis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
    FastAPICache.init(RedisBackend(redis), prefix="neo4j-props")

# Preload collections at startup
node_collections = []
@app.on_event("startup")
async def load_collections():
    global node_collections
    colls = await db.list_collection_names()
    node_collections = [col for col in colls if col.startswith('nodes_')]
    # Ensure indexes exist
    for col in node_collections:
        await db[col].create_index("id")

@app.get("/mongo", summary="Get node properties by ID")
@cache(expire=300)  # Cache for 5 minutes
async def get_properties(
    id: str = Query(..., description="Node ID to retrieve properties for"),
    minimal: bool = Query(False, description="Return only essential properties for visualization")
) -> Dict[str, Any]:
    """
    Retrieves properties from MongoDB in a Neo4j-visualization-friendly format.
    """
    properties = {}
    
    for collection_name in node_collections:
        try:
            node = await db[collection_name].find_one({"id": id}, {"_id": 0})
            if node:
                if minimal:
                    # Only return properties useful for visualization
                    properties.update({
                        k: v for k, v in node.items() 
                        if k in ['id', 'name', 'type', 'label', 'color']
                    })
                else:
                    properties.update(node)
        except Exception as e:
            print(f"Error querying {collection_name}: {e}")
            continue

    if not properties:
        raise HTTPException(status_code=404, detail="Node not found")
    
    return {"node": properties}  # Wrapped for better Neo4j compatibility

@app.get("/mongo/bulk", summary="Get multiple nodes' properties")
@cache(expire=300)
async def get_bulk_properties(
    ids: str = Query(..., description="Comma-separated list of node IDs"),
    minimal: bool = Query(False)
) -> Dict[str, Dict[str, Any]]:
    """
    Bulk retrieval endpoint optimized for Neo4j visualization.
    """
    id_list = [id.strip() for id in ids.split(",")]
    results = {}
    
    for collection_name in node_collections:
        try:
            cursor = db[collection_name].find({"id": {"$in": id_list}}, {"_id": 0})
            async for doc in cursor:
                if minimal:
                    filtered = {
                        k: v for k, v in doc.items() 
                        if k in ['id', 'name', 'type', 'label', 'color']
                    }
                    if doc["id"] not in results:
                        results[doc["id"]] = {"node": filtered}
                    else:
                        results[doc["id"]]["node"].update(filtered)
                else:
                    if doc["id"] not in results:
                        results[doc["id"]] = {"node": doc}
                    else:
                        results[doc["id"]]["node"].update(doc)
        except Exception as e:
            print(f"Error querying {collection_name}: {e}")
            continue

    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=4)