import pymongo
from pymongo import MongoClient
from typing import List, Dict, Any
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import logging
from datetime import datetime


def _build_mongo_uri(mongo_connection) -> str:
    """
    Build a MongoDB connection URI from an Airflow Connection.

    Supports the following sources, in order of precedence:
    1) extra["uri" | "mongodb_uri" | "connection_uri"], if present
    2) connection.host if it already starts with mongodb:// or mongodb+srv:// (treat as full URI)
    3) username/password/host/port/auth_source/ssl
    4) Airflow Variable "BREWLYTIX_MONGODB_URI" as last fallback
    """
    # 1) Check extras for a direct URI
    extras = getattr(mongo_connection, "extra_dejson", {}) or {}
    for key in ["uri", "mongodb_uri", "connection_uri"]:
        if extras.get(key):
            return extras.get(key)

    # 2) If host is itself a full URI, use it directly
    host = mongo_connection.host or extras.get("host")
    if host and (host.startswith("mongodb://") or host.startswith("mongodb+srv://")):
        return host

    # 3) Build from discrete fields
    port = mongo_connection.port or extras.get("port") or 27017
    username = mongo_connection.login
    password = mongo_connection.password
    auth_source = extras.get('auth_source') or extras.get('authSource') or 'admin'
    ssl = extras.get('ssl', False) or extras.get('tls', False)

    if host:
        if username and password:
            uri = f"mongodb://{username}:{password}@{host}:{port}/{auth_source}"
        else:
            uri = f"mongodb://{host}:{port}"
        if ssl:
            # append query param safely
            uri = uri + ("?" if "?" not in uri else "&") + "ssl=true"
        return uri

    # 4) Fallback to Airflow Variable if available
    try:
        from airflow.models import Variable
        uri = Variable.get("BREWLYTIX_MONGODB_URI")
        if uri:
            return uri
    except Exception:
        pass

    raise ValueError("❌ MongoDB connection is missing host/URI. Set host to a full mongodb URI, add 'uri' in extras, or define Variable 'BREWLYTIX_MONGODB_URI'.")


def load_beerxml_to_mongodb(recipes_data: List[Dict[str, Any]], 
                           database_name: str = "brewlytix", 
                           collection_name: str = "recipes",
                           connection_id: str = "brewlytix-mongodb") -> None:
    """
    Load flattened BeerXML recipe data to MongoDB.
    
    Args:
        recipes_data: List of flattened recipe dictionaries
        database_name: MongoDB database name (default: "brewlytix")
        collection_name: MongoDB collection name (default: "recipes")
        connection_id: Airflow connection ID for MongoDB (default: "brewlytix-mongodb")
    """
    if not recipes_data:
        print("❌ No recipe data to load to MongoDB")
        return
    
    print(f"🔄 Loading {len(recipes_data)} recipes to MongoDB...")
    print(f"📊 Database: {database_name}, Collection: {collection_name}")
    print(f"🔗 Using MongoDB connection: {connection_id}")
    
    try:
        # Get MongoDB connection from Airflow
        mongo_connection = BaseHook.get_connection(connection_id)
        print(f"✅ Successfully retrieved MongoDB connection: {connection_id}")
    except Exception as e:
        print(f"❌ Failed to retrieve MongoDB connection '{connection_id}': {str(e)}")
        raise
    
    # Build MongoDB connection string (robust to different configurations)
    connection_uri = _build_mongo_uri(mongo_connection)
    print(f"🔑 Connecting to MongoDB using URI: {connection_uri.split('@')[-1]}")
    
    try:
        # Connect to MongoDB
        client = MongoClient(connection_uri)
        
        # Test connection
        client.admin.command('ping')
        print(f"✅ Successfully connected to MongoDB")
        
        # Default database from connection schema/extras if not provided
        if not database_name:
            schema_db = getattr(mongo_connection, "schema", None)
            if not schema_db:
                schema_db = (mongo_connection.extra_dejson or {}).get("database")
            database_name = schema_db or "brewlytix"

        # Get database and collection
        db = client[database_name]
        collection = db[collection_name]
        
        # Prepare data for insertion
        documents_to_insert = []
        for recipe in recipes_data:
            # Add metadata for tracking
            recipe['_etl_metadata'] = {
                'loaded_at': datetime.now().isoformat(),
                'source': 'beerxml_etl',
                'version': '1.0'
            }
            documents_to_insert.append(recipe)
        
        # Insert documents
        if documents_to_insert:
            # Use insert_many for better performance
            result = collection.insert_many(documents_to_insert)
            print(f"✅ Successfully inserted {len(result.inserted_ids)} recipes into MongoDB")
            
            # Create indexes for better query performance
            create_mongodb_indexes(collection)
            
            # Print summary statistics
            print_mongodb_summary(collection, len(documents_to_insert))
        else:
            print("⚠️ No documents to insert")
        
        # Close connection
        client.close()
        print(f"✅ MongoDB connection closed")
        
    except Exception as e:
        print(f"❌ Error loading data to MongoDB: {str(e)}")
        raise


def create_mongodb_indexes(collection) -> None:
    """
    Create indexes on the MongoDB collection for better query performance.
    
    Args:
        collection: MongoDB collection object
    """
    try:
        print("🔧 Creating MongoDB indexes...")
        
        # Create indexes
        indexes = [
            ("org_id", 1),
            ("name", 1),
            ("type", 1),
            ("brewer", 1),
            ("style.name", 1),
            ("audit.created_at", 1),
            ("provenance.source_record_id", 1),
            ("provenance.imported_at", 1),
            ("estimates.abv_pct", 1),
            ("labels", 1)
        ]
        
        for field, direction in indexes:
            try:
                collection.create_index([(field, direction)])
                print(f"   ✅ Created index on {field}")
            except Exception as e:
                print(f"   ⚠️ Could not create index on {field}: {e}")
        
        # Create compound indexes
        compound_indexes = [
            [("org_id", 1), ("name", 1)],
            [("org_id", 1), ("type", 1)],
            [("org_id", 1), ("brewer", 1)],
            [("org_id", 1), ("style.name", 1)],
            # Versioned unique-ish lookup (not unique because org_id may repeat)
            [("org_id", 1), ("name", 1), ("version", -1)]
        ]

        # Text search index across key fields
        try:
            collection.create_index([
                ("name", "text"),
                ("brewer", "text"),
                ("style.name", "text"),
                ("fermentables.name", "text"),
                ("hops.name", "text")
            ])
            print("   ✅ Created text index on name, brewer, style.name, fermentables.name, hops.name")
        except Exception as e:
            print(f"   ⚠️ Could not create text index: {e}")
        
        for compound_index in compound_indexes:
            try:
                collection.create_index(compound_index)
                print(f"   ✅ Created compound index on {[field for field, _ in compound_index]}")
            except Exception as e:
                print(f"   ⚠️ Could not create compound index: {e}")
        
        print(f"✅ MongoDB indexing completed")
        
    except Exception as e:
        print(f"⚠️ Error creating MongoDB indexes: {e}")


def print_mongodb_summary(collection, inserted_count: int) -> None:
    """
    Print summary statistics of the MongoDB collection.
    
    Args:
        collection: MongoDB collection object
        inserted_count: Number of documents inserted
    """
    try:
        # Get collection statistics
        total_count = collection.count_documents({})
        
        # Get unique counts
        unique_brewers = len(collection.distinct("brewer"))
        unique_styles = len(collection.distinct("style.name"))
        unique_types = len(collection.distinct("type"))
        
        # Get recipe type distribution
        type_pipeline = [
            {"$group": {"_id": "$type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        type_distribution = list(collection.aggregate(type_pipeline))
        
        # Get top brewers
        brewer_pipeline = [
            {"$group": {"_id": "$brewer", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        top_brewers = list(collection.aggregate(brewer_pipeline))
        
        print(f"📊 MongoDB Load Summary:")
        print(f"   - Total recipes in collection: {total_count}")
        print(f"   - Recipes inserted in this run: {inserted_count}")
        print(f"   - Unique brewers: {unique_brewers}")
        print(f"   - Unique styles: {unique_styles}")
        print(f"   - Recipe types: {unique_types}")
        
        if type_distribution:
            print(f"   - Recipe type distribution:")
            for type_info in type_distribution:
                print(f"     * {type_info['_id']}: {type_info['count']}")
        
        if top_brewers:
            print(f"   - Top brewers by recipe count:")
            for brewer in top_brewers:
                print(f"     * {brewer['_id']}: {brewer['count']}")
        
    except Exception as e:
        print(f"⚠️ Error generating MongoDB summary: {e}")


def validate_mongodb_connection(connection_id: str = "brewlytix-mongodb") -> bool:
    """
    Validate MongoDB connection without performing any operations.
    
    Args:
        connection_id: Airflow connection ID for MongoDB
    
    Returns:
        True if connection is valid, False otherwise
    """
    try:
        # Get MongoDB connection from Airflow and build URI
        mongo_connection = BaseHook.get_connection(connection_id)
        connection_uri = _build_mongo_uri(mongo_connection)
        
        # Test connection
        client = MongoClient(connection_uri)
        client.admin.command('ping')
        client.close()
        
        print(f"✅ MongoDB connection validation successful")
        return True
        
    except Exception as e:
        print(f"❌ MongoDB connection validation failed: {str(e)}")
        return False


def get_mongodb_collection_stats(database_name: str = "brewlytix", 
                                collection_name: str = "recipes",
                                connection_id: str = "brewlytix-mongodb") -> Dict[str, Any]:
    """
    Get statistics about the MongoDB collection.
    
    Args:
        database_name: MongoDB database name
        collection_name: MongoDB collection name
        connection_id: Airflow connection ID for MongoDB
    
    Returns:
        Dictionary containing collection statistics
    """
    try:
        # Get MongoDB connection and build URI
        mongo_connection = BaseHook.get_connection(connection_id)
        connection_uri = _build_mongo_uri(mongo_connection)
        
        # Connect and get stats
        client = MongoClient(connection_uri)
        db = client[database_name]
        collection = db[collection_name]
        
        # Get collection stats
        stats = db.command("collStats", collection_name)
        
        # Get document counts
        total_docs = collection.count_documents({})
        unique_brewers = len(collection.distinct("brewer"))
        unique_styles = len(collection.distinct("style.name"))
        
        client.close()
        
        return {
            "total_documents": total_docs,
            "unique_brewers": unique_brewers,
            "unique_styles": unique_styles,
            "collection_size_bytes": stats.get("size", 0),
            "average_document_size": stats.get("avgObjSize", 0),
            "indexes": stats.get("nindexes", 0)
        }
        
    except Exception as e:
        print(f"❌ Error getting MongoDB collection stats: {str(e)}")
        return {}
