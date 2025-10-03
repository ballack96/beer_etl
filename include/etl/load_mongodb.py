import pymongo
from pymongo import MongoClient
from typing import List, Dict, Any
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus


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
        # For full URIs, we might need to handle special characters in the URI itself
        # If it's a full URI, use it as-is (it should already be properly encoded)
        # But we need to ensure it doesn't have $external as database name
        if "/$external" in host:
            # Replace /$external with proper query parameter
            host = host.replace("/$external", "")
            if "?" in host:
                host += "&authSource=%24external"
            else:
                host += "?authSource=%24external"
        return host

    # 3) Build from discrete fields
    port = mongo_connection.port or extras.get("port") or 27017
    username = mongo_connection.login
    password = mongo_connection.password
    auth_source = extras.get('auth_source') or extras.get('authSource') or 'admin'
    ssl = extras.get('ssl', False) or extras.get('tls', False)
    
    # Get database name from connection schema or extras
    database_name = mongo_connection.schema or extras.get('database') or 'admin'

    if host:
        if username and password:
            # URL encode username and password to handle special characters
            encoded_username = quote_plus(username)
            encoded_password = quote_plus(password)
            
            # For Atlas clusters, use mongodb+srv:// protocol
            if "mongodb.net" in host or "atlas" in host.lower():
                # Use mongodb+srv:// for Atlas connections with SCRAM auth
                uri = f"mongodb+srv://{encoded_username}:{encoded_password}@{host}"
            else:
                # For regular MongoDB connections
                if auth_source == "$external":
                    uri = f"mongodb://{encoded_username}:{encoded_password}@{host}:{port}"
                else:
                    uri = f"mongodb://{encoded_username}:{encoded_password}@{host}:{port}/{auth_source}"
        else:
            # For Atlas clusters, use mongodb+srv:// protocol
            if "mongodb.net" in host or "atlas" in host.lower():
                uri = f"mongodb+srv://{host}"
            else:
                uri = f"mongodb://{host}:{port}"
        
        # Add query parameters
        query_params = []
        if auth_source and auth_source != "admin":
            query_params.append(f"authSource={quote_plus(auth_source)}")
        if ssl:
            query_params.append("ssl=true")
        
        # Add Atlas-specific parameters
        if "mongodb.net" in host or "atlas" in host.lower():
            query_params.extend(["retryWrites=true", "w=majority", "appName=brewlytix-mongo"])
        
        if query_params:
            uri += "?" + "&".join(query_params)
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


def ensure_datetime_objects(recipe: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure all timestamp fields are proper datetime objects for MongoDB.
    
    Args:
        recipe: Recipe dictionary
    
    Returns:
        Recipe with proper datetime objects
    """
    from datetime import datetime, timezone
    
    # Fix audit timestamps
    if "audit" in recipe:
        audit = recipe["audit"]
        if "created_at" in audit and audit["created_at"]:
            if isinstance(audit["created_at"], str):
                # Convert string to datetime if needed
                try:
                    audit["created_at"] = datetime.fromisoformat(audit["created_at"].replace('Z', '+00:00'))
                except:
                    audit["created_at"] = datetime.now(timezone.utc)
        
        if "updated_at" in audit and audit["updated_at"]:
            if isinstance(audit["updated_at"], str):
                try:
                    audit["updated_at"] = datetime.fromisoformat(audit["updated_at"].replace('Z', '+00:00'))
                except:
                    audit["updated_at"] = datetime.now(timezone.utc)
    
    # Fix provenance timestamps
    if "provenance" in recipe:
        provenance = recipe["provenance"]
        if "imported_at" in provenance and provenance["imported_at"]:
            if isinstance(provenance["imported_at"], str):
                try:
                    provenance["imported_at"] = datetime.fromisoformat(provenance["imported_at"].replace('Z', '+00:00'))
                except:
                    provenance["imported_at"] = datetime.now(timezone.utc)
    
    return recipe


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
        
        # Prepare data for insertion with datetime objects
        documents_to_insert = []
        for recipe in recipes_data:
            # Ensure all timestamps are proper datetime objects
            recipe = ensure_datetime_objects(recipe)
            
            # Add metadata for tracking
            recipe['_etl_metadata'] = {
                'loaded_at': datetime.now(timezone.utc),
                'source': 'beerxml_etl',
                'version': '1.0'
            }
            documents_to_insert.append(recipe)
        
        # Handle duplicates using upsert operations
        if documents_to_insert:
            # Get duplicate handling strategy from Airflow Variables
            try:
                duplicate_strategy = Variable.get("BREWLYTIX_DUPLICATE_STRATEGY", default_var="upsert")
            except Exception:
                duplicate_strategy = "upsert"
            
            print(f"🔄 Using duplicate handling strategy: {duplicate_strategy}")
            
            if duplicate_strategy == "upsert":
                # Use upsert operations to handle duplicates
                upsert_results = upsert_recipes_to_mongodb(collection, documents_to_insert)
                print(f"✅ Upsert operation completed: {upsert_results}")
            elif duplicate_strategy == "skip":
                # Skip duplicates and only insert new recipes
                skip_results = insert_new_recipes_only(collection, documents_to_insert)
                print(f"✅ Insert new recipes only completed: {skip_results}")
            else:
                # Default to original insert_many behavior
                insert_results = insert_recipes_with_error_handling(collection, documents_to_insert)
                print(f"✅ Insert operation completed: {insert_results}")
            
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


def upsert_recipes_to_mongodb(collection, documents_to_insert: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Upsert recipes to MongoDB, updating existing ones and inserting new ones.
    
    Args:
        collection: MongoDB collection object
        documents_to_insert: List of recipe documents to upsert
    
    Returns:
        Dictionary with operation results
    """
    upserted_count = 0
    matched_count = 0
    modified_count = 0
    
    for recipe in documents_to_insert:
        try:
            # Create filter for unique identification (org_id + name + version)
            filter_query = {
                "org_id": recipe.get("org_id"),
                "name": recipe.get("name"),
                "version": recipe.get("version", 1)
            }
            
            # Update the recipe with current timezone-aware datetime
            from datetime import timezone
            recipe["audit"]["updated_at"] = datetime.now(timezone.utc)
            
            # Perform upsert operation
            result = collection.replace_one(
                filter_query,
                recipe,
                upsert=True
            )
            
            if result.upserted_id:
                upserted_count += 1
                print(f"   ✅ Inserted new recipe: {recipe.get('name')} v{recipe.get('version', 1)}")
            elif result.modified_count > 0:
                modified_count += 1
                print(f"   🔄 Updated existing recipe: {recipe.get('name')} v{recipe.get('version', 1)}")
            else:
                matched_count += 1
                print(f"   ⚪ No changes needed for: {recipe.get('name')} v{recipe.get('version', 1)}")
                
        except Exception as e:
            print(f"   ❌ Error upserting recipe {recipe.get('name', 'unknown')}: {str(e)}")
            continue
    
    return {
        "upserted": upserted_count,
        "modified": modified_count,
        "matched": matched_count,
        "total_processed": len(documents_to_insert)
    }


def insert_new_recipes_only(collection, documents_to_insert: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert only new recipes, skipping duplicates.
    
    Args:
        collection: MongoDB collection object
        documents_to_insert: List of recipe documents to insert
    
    Returns:
        Dictionary with operation results
    """
    new_recipes = []
    skipped_count = 0
    
    # Check for existing recipes first
    for recipe in documents_to_insert:
        filter_query = {
            "org_id": recipe.get("org_id"),
            "name": recipe.get("name"),
            "version": recipe.get("version", 1)
        }
        
        existing = collection.find_one(filter_query)
        if existing:
            skipped_count += 1
            print(f"   ⚪ Skipping duplicate recipe: {recipe.get('name')} v{recipe.get('version', 1)}")
        else:
            new_recipes.append(recipe)
    
    # Insert only new recipes
    if new_recipes:
        try:
            result = collection.insert_many(new_recipes, ordered=False)
            print(f"✅ Inserted {len(result.inserted_ids)} new recipes")
        except pymongo.errors.BulkWriteError as bwe:
            # Handle partial failures
            details = getattr(bwe, 'details', {}) or {}
            write_errors = details.get('writeErrors', [])
            successful_inserts = len(documents_to_insert) - len(write_errors)
            print(f"⚠️ Partial insert success: {successful_inserts} inserted, {len(write_errors)} failed")
    
    return {
        "inserted": len(new_recipes),
        "skipped": skipped_count,
        "total_processed": len(documents_to_insert)
    }


def insert_recipes_with_error_handling(collection, documents_to_insert: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert recipes with comprehensive error handling (original behavior).
    
    Args:
        collection: MongoDB collection object
        documents_to_insert: List of recipe documents to insert
    
    Returns:
        Dictionary with operation results
    """
    # Configurable insert options via Airflow Variables
    try:
        ordered_var = Variable.get("BREWLYTIX_MONGODB_INSERT_ORDERED", default_var="true")
        ordered = str(ordered_var).lower() in ["1", "true", "yes", "y"]
    except Exception:
        ordered = True

    try:
        bypass_var = Variable.get("BREWLYTIX_MONGODB_BYPASS_VALIDATION", default_var="false")
        bypass_validation = str(bypass_var).lower() in ["1", "true", "yes", "y"]
    except Exception:
        bypass_validation = False
    
    print(f"🛠️ insert_many options → ordered={ordered}, bypass_document_validation={bypass_validation}")

    try:
        # Use insert_many for better performance
        result = collection.insert_many(
            documents_to_insert,
            ordered=ordered,
            bypass_document_validation=bypass_validation
        )
        print(f"✅ Successfully inserted {len(result.inserted_ids)} recipes into MongoDB")
        return {"inserted": len(result.inserted_ids), "errors": 0}
    except pymongo.errors.OperationFailure as ofe:
        if "bypassDocumentValidation" in str(ofe):
            print(f"⚠️ User lacks bypassDocumentValidation permission, retrying without bypass...")
            # Retry without bypass_document_validation
            result = collection.insert_many(
                documents_to_insert,
                ordered=ordered
            )
            print(f"✅ Successfully inserted {len(result.inserted_ids)} recipes into MongoDB (without bypass)")
            return {"inserted": len(result.inserted_ids), "errors": 0}
        else:
            raise
    except pymongo.errors.BulkWriteError as bwe:
        details = getattr(bwe, 'details', {}) or {}
        write_errors = details.get('writeErrors', [])
        print(f"❌ BulkWriteError: {len(write_errors)} write error(s) occurred during insert_many")
        for err in write_errors:
            try:
                idx = err.get('index')
                code = err.get('code')
                errmsg = err.get('errmsg', '')
                errinfo = err.get('errInfo', {}) or {}
                print(f"   • index={idx}, code={code}, errmsg={errmsg[:500]}")
                if errinfo:
                    print(f"     errInfo keys: {list(errinfo.keys())}")
                    details_info = errinfo.get('details') or errinfo
                    # Print a compact view of JSON schema validation details if present
                    try:
                        # Some MongoDB versions include a nested 'schemaRulesNotSatisfied'
                        violated = details_info.get('schemaRulesNotSatisfied')
                        if violated:
                            print(f"     schemaRulesNotSatisfied count: {len(violated)}")
                    except Exception:
                        pass
                # Summarize the failing document's field types for quick diagnosis
                if isinstance(idx, int) and 0 <= idx < len(documents_to_insert):
                    doc = documents_to_insert[idx]
                    type_summary = {k: type(v).__name__ for k, v in doc.items()}
                    # Avoid dumping entire document; just show keys and types
                    print(f"     failingDocument field types: {type_summary}")
            except Exception as ie:
                print(f"   ⚠️ Error while logging BulkWriteError details: {ie}")
        # Re-raise for upstream handling since some documents were not inserted
        raise


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
