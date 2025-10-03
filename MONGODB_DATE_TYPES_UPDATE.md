# MongoDB Date Types Implementation

This document outlines the changes made to use native MongoDB Date types instead of string timestamps throughout the Beer ETL pipeline.

## Problem Summary

The previous implementation used ISO string timestamps which caused issues with:
- Go API parsing (timezone format requirements)
- MongoDB query performance
- Date range operations
- Timezone handling inconsistencies

## Solution: Native MongoDB Date Types

### 1. Updated Flattening Process (`include/etl/flatten.py`)

#### Key Changes:
- **Replaced string timestamps with datetime objects**
- **Added timezone support** via `get_timezone_aware_datetime()`
- **Enhanced function signatures** to accept timezone parameters

#### Before (String Timestamps):
```python
def get_timezone_aware_timestamp(timezone_name: str = "UTC") -> str:
    return datetime.now(tz).isoformat()

# Usage
"imported_at": get_timezone_aware_timestamp("UTC"),  # Returns string
```

#### After (Date Objects):
```python
def get_timezone_aware_datetime(timezone_name: str = "UTC") -> datetime:
    return datetime.now(tz)

# Usage
"imported_at": get_timezone_aware_datetime(timezone_name),  # Returns datetime object
```

### 2. Updated MongoDB Schema (`playground-1.mongodb.js`)

#### Schema Changes:
```javascript
// Before: Mixed string/date types
audit: {
  created_at: { bsonType: ["date", "string"] },
  updated_at: { bsonType: ["date", "string", "null"] },
}

// After: Pure Date types
audit: {
  created_at: { bsonType: "date" },
  updated_at: { bsonType: ["date", "null"] },
}

// Provenance also updated
provenance: {
  imported_at: { bsonType: "date" },  // Was ["date", "string", "null"]
}
```

### 3. Updated Load Function (`include/etl/load_mongodb.py`)

#### Key Changes:
- **Replaced `fix_timezone_timestamps()` with `ensure_datetime_objects()`**
- **Enhanced datetime conversion** with proper error handling
- **Updated metadata** to use datetime objects

#### New Function:
```python
def ensure_datetime_objects(recipe: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure all timestamp fields are proper datetime objects for MongoDB."""
    # Converts string timestamps to datetime objects
    # Handles timezone conversion
    # Provides fallback for invalid dates
```

### 4. Updated DAG (`dags/beerxml_etl_dag.py`)

#### Enhanced Configuration:
- **Timezone parameter passing** to flattening process
- **Better error handling** for datetime conversion
- **Improved logging** of timezone usage

## Benefits of MongoDB Date Types

### 1. **Performance Improvements**
- **Faster queries** on date fields
- **Better indexing** performance
- **Efficient date range queries**

### 2. **API Compatibility**
- **Go time parsing** works seamlessly
- **No timezone format issues**
- **Consistent date handling**

### 3. **MongoDB Features**
- **Native date operations** (aggregation, sorting)
- **Date range queries** with proper indexing
- **Timezone-aware operations**

### 4. **Data Integrity**
- **Type safety** at database level
- **Consistent date format** across all records
- **Better validation** through schema

## Usage Examples

### 1. Querying with Date Types
```javascript
// MongoDB queries now work with native date types
db.recipes.find({
  "audit.created_at": { $gte: new Date("2025-01-01") }
})

// Date range queries
db.recipes.find({
  "provenance.imported_at": {
    $gte: new Date("2025-01-01"),
    $lt: new Date("2025-02-01")
  }
})
```

### 2. Go API Integration
```go
// Go can now parse MongoDB dates directly
type Recipe struct {
    Audit struct {
        CreatedAt time.Time `bson:"created_at" json:"created_at"`
        UpdatedAt *time.Time `bson:"updated_at" json:"updated_at"`
    } `bson:"audit" json:"audit"`
}
```

### 3. Aggregation Queries
```javascript
// Date-based aggregations work efficiently
db.recipes.aggregate([
  {
    $group: {
      _id: { $dateToString: { format: "%Y-%m", date: "$audit.created_at" } },
      count: { $sum: 1 }
    }
  }
])
```

## Configuration

### Airflow Variables
```bash
# Timezone configuration (defaults to UTC)
BREWLYTIX_TIMEZONE=UTC

# Duplicate handling strategy
BREWLYTIX_DUPLICATE_STRATEGY=upsert
```

### Supported Timezones
- `UTC` (default)
- `America/New_York`
- `Europe/London`
- `Asia/Tokyo`
- Any valid `pytz` timezone

## Migration Steps

### 1. **Update Dependencies**
```bash
pip install pytz pymongo
```

### 2. **Update MongoDB Schema**
```bash
# Run the updated playground script
mongo < playground-1.mongodb.js
```

### 3. **Set Airflow Variables**
```bash
airflow variables set BREWLYTIX_TIMEZONE "UTC"
airflow variables set BREWLYTIX_DUPLICATE_STRATEGY "upsert"
```

### 4. **Test the Pipeline**
```bash
# Run the DAG
airflow dags trigger beerxml_etl_dag

# Check the results
mongo brewlytix --eval "db.recipes.findOne({}, {audit: 1, provenance: 1})"
```

## Expected Results

### 1. **MongoDB Documents**
```javascript
{
  "audit": {
    "created_at": ISODate("2025-01-15T10:30:45.123Z"),
    "updated_at": null
  },
  "provenance": {
    "imported_at": ISODate("2025-01-15T10:30:45.123Z")
  }
}
```

### 2. **Go API Response**
```json
{
  "audit": {
    "created_at": "2025-01-15T10:30:45.123Z",
    "updated_at": null
  },
  "provenance": {
    "imported_at": "2025-01-15T10:30:45.123Z"
  }
}
```

### 3. **Performance Benefits**
- **Faster date queries** (indexed properly)
- **Better aggregation performance**
- **Consistent timezone handling**
- **No string parsing overhead**

## Troubleshooting

### Common Issues

1. **Timezone Errors**
   ```bash
   # Check timezone support
   python -c "import pytz; print('UTC' in pytz.all_timezones)"
   ```

2. **Date Conversion Issues**
   ```python
   # Test datetime conversion
   from datetime import datetime, timezone
   print(datetime.now(timezone.utc))
   ```

3. **MongoDB Schema Validation**
   ```javascript
   // Check schema compliance
   db.recipes.find({audit: {$exists: true}}).limit(1)
   ```

### Debug Commands

```bash
# Check MongoDB date types
mongo brewlytix --eval "
  db.recipes.findOne({}, {audit: 1, provenance: 1})
"

# Verify timezone handling
python -c "
from datetime import datetime, timezone
import pytz
tz = pytz.timezone('UTC')
print(datetime.now(tz))
"
```

## Future Enhancements

- **Automatic timezone detection** from recipe metadata
- **Date range validation** in schema
- **Timezone conversion** for different regions
- **Date-based partitioning** for large datasets
- **Automated date cleanup** for historical data

## Summary

The migration to MongoDB Date types provides:

✅ **Better Performance** - Native date operations and indexing  
✅ **API Compatibility** - Seamless Go time parsing  
✅ **Data Integrity** - Type safety and validation  
✅ **Timezone Support** - Configurable timezone handling  
✅ **MongoDB Features** - Full date query capabilities  

Your Go REST API should now work perfectly with the native MongoDB Date types! 🎉
