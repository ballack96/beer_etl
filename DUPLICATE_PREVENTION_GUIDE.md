# Duplicate Prevention Guide for Beer ETL Pipeline

This guide explains the duplicate prevention strategies implemented in the Beer ETL pipeline.

## Overview

The pipeline now includes comprehensive duplicate prevention at multiple levels:

1. **Database Level**: Unique indexes prevent duplicate recipes
2. **Application Level**: Configurable duplicate handling strategies
3. **ETL Level**: Smart upsert operations

## Duplicate Prevention Strategies

### 1. Database Level (MongoDB)

**Unique Index**: `{ org_id: 1, name: 1, version: 1 }`
- Prevents duplicate recipes within the same organization
- Ensures recipe uniqueness based on org_id + name + version combination
- Applied in `playground-1.mongodb.js`

### 2. Application Level (ETL Pipeline)

Three configurable strategies via Airflow Variable `BREWLYTIX_DUPLICATE_STRATEGY`:

#### Strategy 1: Upsert (Default)
- **Value**: `upsert`
- **Behavior**: Update existing recipes, insert new ones
- **Use Case**: When you want to keep recipes up-to-date
- **Implementation**: Uses MongoDB `replace_one()` with `upsert=True`

#### Strategy 2: Skip Duplicates
- **Value**: `skip`
- **Behavior**: Only insert new recipes, skip existing ones
- **Use Case**: When you want to preserve existing data
- **Implementation**: Checks for existing recipes before insertion

#### Strategy 3: Fail on Duplicates
- **Value**: `insert`
- **Behavior**: Original behavior - fail if duplicates found
- **Use Case**: When you want to be alerted to duplicates
- **Implementation**: Uses `insert_many()` with error handling

## Configuration

### Airflow Variables

Set these variables in your Airflow UI:

```bash
# Duplicate handling strategy (upsert|skip|insert)
BREWLYTIX_DUPLICATE_STRATEGY=upsert

# MongoDB connection settings
BREWLYTIX_DB_NAME=brewlytix
BREWLYTIX_COLLECTION_NAME=recipes
BREWLYTIX_MONGODB_CONNECTION_ID=brewlytix-atlas-scram

# Optional: MongoDB insert options
BREWLYTIX_MONGODB_INSERT_ORDERED=true
BREWLYTIX_MONGODB_BYPASS_VALIDATION=false
```

### MongoDB Schema Setup

Run the updated `playground-1.mongodb.js` to set up:
- Unique compound index on `{ org_id: 1, name: 1, version: 1 }`
- Performance indexes for common queries
- Text search indexes for full-text search

## Implementation Details

### Key Functions Added

1. **`upsert_recipes_to_mongodb()`**
   - Handles upsert operations
   - Updates existing recipes with new data
   - Inserts new recipes
   - Provides detailed logging

2. **`insert_new_recipes_only()`**
   - Checks for existing recipes before insertion
   - Skips duplicates
   - Only inserts new recipes

3. **`insert_recipes_with_error_handling()`**
   - Original behavior with comprehensive error handling
   - Handles BulkWriteError and OperationFailure
   - Provides detailed error reporting

### Duplicate Identification

Recipes are considered duplicates if they have the same:
- `org_id` (organization ID)
- `name` (recipe name)
- `version` (recipe version)

## Usage Examples

### Example 1: Update Existing Recipes (Upsert)
```python
# Set Airflow Variable
BREWLYTIX_DUPLICATE_STRATEGY=upsert

# Run the DAG
# Existing recipes will be updated with new data
# New recipes will be inserted
```

### Example 2: Skip Duplicates
```python
# Set Airflow Variable
BREWLYTIX_DUPLICATE_STRATEGY=skip

# Run the DAG
# Only new recipes will be inserted
# Existing recipes will be skipped
```

### Example 3: Fail on Duplicates
```python
# Set Airflow Variable
BREWLYTIX_DUPLICATE_STRATEGY=insert

# Run the DAG
# Will fail if any duplicates are found
# Use this to detect duplicate issues
```

## Monitoring and Logging

The pipeline provides detailed logging for duplicate handling:

```
🔄 Using duplicate handling strategy: upsert
   ✅ Inserted new recipe: American Pale Ale v1
   🔄 Updated existing recipe: IPA v2
   ⚪ No changes needed for: Stout v1
✅ Upsert operation completed: {'upserted': 5, 'modified': 3, 'matched': 2, 'total_processed': 10}
```

## Best Practices

1. **Use Upsert for Production**: Default strategy handles most use cases
2. **Monitor Logs**: Check for unexpected duplicate patterns
3. **Test Strategies**: Try different strategies in development
4. **Index Maintenance**: Ensure unique indexes are properly created
5. **Data Quality**: Validate data before ETL runs

## Troubleshooting

### Common Issues

1. **Duplicate Key Error**: Check if unique index is properly created
2. **Permission Errors**: Ensure MongoDB user has upsert permissions
3. **Schema Validation**: Check if documents match MongoDB schema
4. **Connection Issues**: Verify MongoDB connection settings

### Debug Steps

1. Check Airflow Variables are set correctly
2. Verify MongoDB connection and permissions
3. Review ETL logs for specific error messages
4. Test with small dataset first
5. Check MongoDB indexes with `db.recipes.getIndexes()`

## Migration from Old System

If you're migrating from a system without duplicate prevention:

1. **Backup Data**: Always backup before making changes
2. **Test Strategy**: Use `skip` strategy first to test
3. **Monitor Results**: Check logs for unexpected behavior
4. **Switch to Upsert**: Once confident, switch to `upsert`
5. **Clean Up**: Remove any duplicate data if needed

## Performance Considerations

- **Upsert**: Slower for large datasets (individual operations)
- **Skip**: Faster for new data (bulk operations)
- **Insert**: Fastest but fails on duplicates
- **Indexes**: Ensure proper indexing for performance
- **Batch Size**: Consider MongoDB batch size limits

## Future Enhancements

Potential improvements:
- Bulk upsert operations for better performance
- Conflict resolution strategies (latest wins, merge fields)
- Duplicate detection with fuzzy matching
- Data quality metrics and reporting
- Automated duplicate cleanup jobs
