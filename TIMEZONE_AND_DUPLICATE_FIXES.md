# Timezone and Duplicate Detection Fixes

This document outlines the fixes implemented to resolve timezone issues and improve duplicate detection in the Beer ETL pipeline.

## Problem Summary

The Go REST API was failing with the error:
```
"failed to decode recipe: error decoding key provenance.imported_at: parsing time 
\"2025-09-29T21:15:31.389859\" as \"2006-01-02T15:04:05.999Z07:00\": cannot parse \"\" as \"Z07:00\""
```

This was caused by timestamps missing timezone information, which Go's time parsing requires.

## Solutions Implemented

### 1. Timezone-Aware Timestamps

#### Updated `include/etl/flatten.py`
- **Added timezone support**: Import `pytz` and `timezone` modules
- **New function**: `get_timezone_aware_timestamp()` - Creates ISO 8601 timestamps with timezone info
- **Updated timestamp creation**: All timestamps now include timezone information (UTC by default)

```python
def get_timezone_aware_timestamp(timezone_name: str = "UTC") -> str:
    """Get a timezone-aware timestamp in ISO 8601 format."""
    try:
        tz = pytz.timezone(timezone_name)
        now = datetime.now(tz)
        return now.isoformat()
    except Exception:
        # Fallback to UTC if timezone is invalid
        now = datetime.now(timezone.utc)
        return now.isoformat()
```

#### Updated `include/etl/load_mongodb.py`
- **Added timezone fix function**: `fix_timezone_timestamps()` - Ensures all timestamps have timezone info
- **Updated load process**: All recipes are processed through timezone validation before insertion
- **Enhanced metadata**: ETL metadata now includes timezone-aware timestamps

```python
def fix_timezone_timestamps(recipe: Dict[str, Any]) -> Dict[str, Any]:
    """Fix timezone issues in recipe timestamps."""
    # Ensures all timestamps end with 'Z' for UTC or timezone offset
    # Fixes audit.created_at, audit.updated_at, provenance.imported_at
```

### 2. Enhanced Duplicate Detection

#### Content-Based Duplicate Detection
- **New function**: `generate_recipe_hash()` - Creates SHA256 hash based on recipe content
- **Hash includes**: name, brewer, type, version, batch details, fermentables, hops, yeasts
- **Duplicate prevention**: Recipes with identical content hashes are skipped during flattening

#### Database-Level Duplicate Prevention
- **Unique index**: `{ org_id: 1, name: 1, version: 1 }` prevents duplicate recipes
- **Content hash index**: `{ content_hash: 1 }` for fast duplicate lookups
- **Multiple strategies**: upsert, skip, or fail on duplicates (configurable)

### 3. Updated MongoDB Schema

#### Enhanced Schema (`playground-1.mongodb.js`)
- **Added content_hash field**: For duplicate detection
- **Updated indexes**: Added content_hash index for performance
- **Maintained unique constraints**: Prevents duplicate recipes at database level

```javascript
// Added to schema
content_hash: { bsonType: ["string", "null"] },

// Added index
db.recipes.createIndex({ content_hash: 1 });
```

### 4. ETL Pipeline Enhancements

#### Updated DAG (`dags/beerxml_etl_dag.py`)
- **Timezone configuration**: Added `BREWLYTIX_TIMEZONE` Airflow Variable
- **Duplicate detection**: Content-based duplicate detection during flattening
- **Enhanced logging**: Reports duplicates skipped and timezone used
- **Better error handling**: Graceful handling of timezone and duplicate issues

#### Configuration Variables
```bash
# Timezone configuration
BREWLYTIX_TIMEZONE=UTC  # or America/New_York, Europe/London, etc.

# Duplicate handling strategy
BREWLYTIX_DUPLICATE_STRATEGY=upsert  # or skip, insert
```

## Usage Examples

### 1. Fix Existing Data
To fix existing data with timezone issues, run the updated ETL pipeline:

```bash
# Set timezone (optional, defaults to UTC)
airflow variables set BREWLYTIX_TIMEZONE "UTC"

# Set duplicate strategy
airflow variables set BREWLYTIX_DUPLICATE_STRATEGY "upsert"

# Run the DAG
airflow dags trigger beerxml_etl_dag
```

### 2. Verify Timezone Fixes
Check that timestamps now include timezone information:

```javascript
// MongoDB query to check timestamps
db.recipes.find({}, {
  "audit.created_at": 1,
  "provenance.imported_at": 1,
  "content_hash": 1
}).limit(5);
```

Expected format: `2025-01-15T10:30:45.123456+00:00` or `2025-01-15T10:30:45.123456Z`

### 3. Monitor Duplicate Detection
The ETL pipeline now reports duplicate detection:

```
📈 Flattening summary:
   - Total recipes processed: 150
   - Duplicates skipped: 5
   - Timezone: UTC
```

## API Compatibility

### Go Time Parsing
The timestamps now work with Go's time parsing:

```go
// This will now work correctly
importedAt, err := time.Parse(time.RFC3339, "2025-01-15T10:30:45.123456Z")
```

### REST API Response
Your Go REST API should now successfully parse timestamps without errors.

## Performance Considerations

### Indexes Added
- `content_hash`: Fast duplicate detection
- `{ org_id: 1, name: 1, version: 1 }`: Unique constraint
- Existing performance indexes maintained

### Memory Usage
- Content hashing adds minimal memory overhead
- Duplicate detection uses in-memory hash set during processing
- No significant performance impact

## Troubleshooting

### Common Issues

1. **Timezone Errors**
   - Ensure `pytz` is installed: `pip install pytz`
   - Check timezone name is valid: `pytz.all_timezones`

2. **Duplicate Detection Issues**
   - Check content_hash field is populated
   - Verify unique indexes are created
   - Monitor ETL logs for duplicate counts

3. **Go API Still Failing**
   - Verify timestamps include timezone info (end with 'Z' or offset)
   - Check Go time parsing format matches RFC3339
   - Test with sample timestamp: `2025-01-15T10:30:45.123456Z`

### Debug Commands

```bash
# Check timezone support
python -c "import pytz; print(pytz.all_timezones[:5])"

# Test timestamp generation
python -c "
from datetime import datetime, timezone
print(datetime.now(timezone.utc).isoformat())
"

# Check MongoDB indexes
db.recipes.getIndexes()
```

## Migration Steps

1. **Update Dependencies**
   ```bash
   pip install pytz
   ```

2. **Set Airflow Variables**
   ```bash
   airflow variables set BREWLYTIX_TIMEZONE "UTC"
   airflow variables set BREWLYTIX_DUPLICATE_STRATEGY "upsert"
   ```

3. **Update MongoDB Schema**
   - Run updated `playground-1.mongodb.js`
   - Verify indexes are created

4. **Test ETL Pipeline**
   - Run the DAG
   - Check logs for timezone and duplicate information
   - Verify API can parse timestamps

5. **Monitor Results**
   - Check duplicate detection is working
   - Verify timestamps have timezone info
   - Test Go API endpoints

## Future Enhancements

- **Fuzzy duplicate detection**: Handle minor variations in recipe content
- **Timezone conversion**: Support multiple timezones for different regions
- **Batch duplicate cleanup**: Automated cleanup of historical duplicates
- **Content similarity**: Advanced duplicate detection using ML techniques
