import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, LongType, MapType, VariantType
import json
import sys

# Define the table configurations with custom EXCEPT queries
table_configs = [
    {
        "source_table": "SOURCE_DB.SOURCE_SCHEMA.ENTITY_TABLE",
        "hub_table": "DV_DB.RAWVAULT.H_ENTITY",
        "cur_satellite_table": "DV_DB.RAWVAULT.S_ENTITY_LROC_INFO_CURRENT",
        "bizview_table": "DV_DB.BIZVIEWS.FACT_ENTITY",
        "source_key": "ID", 
        "hub_key": "ENTITY_ID",
        "satellite_hash_key": "HK_H_ENTITY", 
        "bizview_key": "ENTITY_ID",
        "deleted_column": "_IS_DELETED",
        "columns_to_compare": [ 
            "NAME", "CODE", "STATUS", "ACTIVE_DATE", "ADDRESS", "CITY", "STATE", "ZIP",
            "PHONE", "EMAIL", "TIMEZONE", "TYPE_ID", "REGION_ID", "MANAGER_ID",
            "REFERENCE_ID", "EXTERNAL_ID", "STATUS_CODE", "CREATED_BY", "UPDATED_BY", 
            "UNIQUE_ID", "_SYNCED_AT"
        ],
        "custom_except_query": """
SELECT ID, NAME, CODE, STATUS, ACTIVE_DATE, ADDRESS, CITY, STATE, ZIP,
       PHONE, EMAIL, TIMEZONE, TYPE_ID, REGION_ID, MANAGER_ID,
       REFERENCE_ID, EXTERNAL_ID, STATUS_CODE, CREATED_BY, UPDATED_BY, 
       UNIQUE_ID, _SYNCED_AT
FROM SOURCE_DB.SOURCE_SCHEMA.ENTITY_TABLE
EXCEPT
SELECT E.ENTITY_ID, NAME, CODE, STATUS, ACTIVE_DATE, ADDRESS, CITY, STATE, ZIP,
       PHONE, EMAIL, TIMEZONE, TYPE_ID, REGION_ID, MANAGER_ID,
       REFERENCE_ID, EXTERNAL_ID, STATUS_CODE, CREATED_BY, UPDATED_BY, 
       UNIQUE_ID, _SYNCED_AT
FROM DV_DB.RAWVAULT.H_ENTITY E
JOIN DV_DB.RAWVAULT.S_ENTITY_LROC_INFO SE ON E.HK_H_ENTITY = SE.HK_H_ENTITY
"""
    }
    # Add more table configurations as needed by copying and modifying the template above
]

def safe_json_conversion(row):
    """
    Safely convert a Snowpark Row or dictionary to a JSON-serializable dictionary
    
    Args:
        row: Snowpark Row or dictionary to convert
    
    Returns:
        Dictionary that can be safely serialized to JSON
    """
    try:
        # If row is already a dictionary, process it
        if isinstance(row, dict):
            return {str(k): (str(v) if not isinstance(v, (dict, list, type(None))) else v) 
                   for k, v in row.items()}
        
        # If row is a Snowpark Row, convert to dictionary first
        if hasattr(row, 'columns'):
            # Convert row to dictionary
            row_dict = dict(zip(row.columns, row))
            processed_dict = {}
            for k, v in row_dict.items():
                # Handle different data types appropriately
                if v is None:
                    processed_dict[str(k)] = None
                elif isinstance(v, (dict, list)):
                    processed_dict[str(k)] = v
                else:
                    # Convert other types to string to ensure JSON serialization
                    processed_dict[str(k)] = str(v)
            return processed_dict
        
        # Fallback conversion
        return {str(k): (str(v) if not isinstance(v, (dict, list, type(None))) else v) 
               for k, v in row.items()}
    
    except Exception as e:
        print(f"Error converting row to JSON-safe dictionary: {str(e)}")
        return {"error": f"Failed to convert row: {str(e)}"}

def extract_missing_records(session, config, limit=10):
    """
    Extract missing records using the EXCEPT query with direct JSON conversion
    
    Args:
        session: Snowflake session
        config: Table configuration dictionary
        limit: Maximum number of records to retrieve
    
    Returns:
        Tuple of (missing_count, list of missing records as JSON dictionaries)
    """
    try:
        # Check if custom EXCEPT query exists
        if not config.get("custom_except_query"):
            print(f"No EXCEPT query for {config.get('source_table', 'UNKNOWN')}")
            return (0, [])
        
        try:
            # Get source and target table names for reference
            source_table = config.get('source_table', 'UNKNOWN_SOURCE')
            hub_table = config.get('hub_table', 'UNKNOWN_HUB')
            satellite_table = config.get('cur_satellite_table', 'UNKNOWN_SATELLITE')
            bizview_table = config.get('bizview_table', 'UNKNOWN_BIZVIEW')
            
            # First get the total count without limit
            count_query = f"SELECT COUNT(*) FROM ({config['custom_except_query']})"
            try:
                missing_count = session.sql(count_query).collect()[0][0]
                print(f"\nTotal Missing Records Count: {missing_count}")
            except Exception as count_error:
                print(f"Error getting total count: {count_error}")
                missing_count = 0
            
            # Modify query to use TO_JSON for direct JSON conversion
            json_query = f"""
            WITH missing_records AS (
                {config['custom_except_query']}
                LIMIT {limit}
            )
            SELECT 
                TO_JSON(OBJECT_CONSTRUCT(*)) AS RECORD_JSON
            FROM missing_records
            """
            
            try:
                # Execute query and collect JSON results
                json_records_df = session.sql(json_query)
                
                # Parse JSON strings into dictionaries
                missing_records = []
                current_group = None
                
                for row in json_records_df.collect():
                    # Each row should have a RECORD_JSON column with JSON string
                    if row.RECORD_JSON:
                        try:
                            # Parse the JSON string into a Python dictionary
                            record_dict = json.loads(row.RECORD_JSON)
                            
                            # Add metadata about tables for easier validation
                            record_dict["__metadata"] = {
                                "source_table": source_table,
                                "hub_table": hub_table,
                                "satellite_table": satellite_table,
                                "bizview_table": bizview_table,
                                "record_type": "missing"
                            }
                            
                            # Add a separator marker between records
                            # This helps with readability when rendered in JSON
                            record_dict["__record_separator"] = True
                            
                            missing_records.append(record_dict)
                        except json.JSONDecodeError as json_err:
                            print(f"Error parsing JSON: {json_err}")
                            missing_records.append({
                                "error": f"Failed to parse JSON: {str(json_err)}",
                                "__metadata": {
                                    "source_table": source_table,
                                    "hub_table": hub_table,
                                    "satellite_table": satellite_table,
                                    "bizview_table": bizview_table
                                },
                                "__record_separator": True
                            })
                    else:
                        missing_records.append({
                            "error": "Empty JSON result",
                            "__metadata": {
                                "source_table": source_table,
                                "hub_table": hub_table,
                                "satellite_table": satellite_table,
                                "bizview_table": bizview_table
                            },
                            "__record_separator": True
                        })
                
                # Add a note about sample size if there are more records than the limit
                if missing_count > limit:
                    note = {
                        "NOTE": f"Showing {limit} of {missing_count} total missing records",
                        "source_table": source_table,
                        "hub_table": hub_table,
                        "satellite_table": satellite_table,
                        "bizview_table": bizview_table
                    }
                    missing_records.append(note)
                
                return (missing_count, missing_records)
            
            except Exception as json_error:
                print(f"Error processing JSON results: {json_error}")
                return (missing_count, [{
                    "error": f"JSON processing error: {str(json_error)}",
                    "__metadata": {
                        "source_table": source_table,
                        "hub_table": hub_table,
                        "satellite_table": satellite_table,
                        "bizview_table": bizview_table
                    }
                }])
        
        except Exception as query_error:
            print(f"Error executing EXCEPT query: {query_error}")
            return (0, [{
                "error": f"Query error: {str(query_error)}",
                "__metadata": {
                    "source_table": config.get('source_table', 'UNKNOWN_SOURCE'),
                    "hub_table": config.get('hub_table', 'UNKNOWN_HUB'),
                    "satellite_table": config.get('cur_satellite_table', 'UNKNOWN_SATELLITE'),
                    "bizview_table": config.get('bizview_table', 'UNKNOWN_BIZVIEW')
                }
            }])
    
    except Exception as e:
        print(f"Critical error in extract_missing_records: {e}")
        return (0, [{
            "error": f"Critical error: {str(e)}",
            "__metadata": {
                "source_table": config.get('source_table', 'UNKNOWN_SOURCE'),
                "hub_table": config.get('hub_table', 'UNKNOWN_HUB'),
                "satellite_table": config.get('cur_satellite_table', 'UNKNOWN_SATELLITE'),
                "bizview_table": config.get('bizview_table', 'UNKNOWN_BIZVIEW')
            }
        }])

def main(session: snowpark.Session):
    """
    Main data validation function
    
    Args:
        session: Snowflake session
    
    Returns:
        Snowpark DataFrame with validation results
    """
    # Define result schema with TOTAL_ROWS_LOST (without ROWS_CHANGED)
    result_schema = StructType([
        StructField("TABLE_NAME", StringType()),
        StructField("SOURCE_TABLE", StringType()),
        StructField("HUB_TABLE", StringType()),
        StructField("SATELLITE_TABLE", StringType()),
        StructField("BIZVIEW_TABLE", StringType()),
        StructField("SOURCE_COUNT", LongType()),
        StructField("HUB_COUNT", LongType()),
        StructField("LINK_COUNT", LongType()),
        StructField("CURRENT_SATELLITE_COUNT", LongType()),
        StructField("BIZVIEW_COUNT", LongType()),
        StructField("SOURCE_TO_HUB_LOSS", LongType()),
        StructField("HUB_TO_LINK_LOSS", LongType()),
        StructField("HUB_TO_SAT_LOSS", LongType()),
        StructField("LINK_TO_SAT_LOSS", LongType()),
        StructField("SAT_TO_BIZVIEW_LOSS", LongType()),
        StructField("TOTAL_ROWS_LOST", LongType()),
        StructField("DELETED_RECORDS", LongType()),
        StructField("LOST_RECORDS_DETAILS", VariantType())
    ])
    
    # Results and lost records containers
    results = []
    lost_records = {}
    
    # Process each table configuration
    for config in table_configs:
        try:
            # Extract table identifiers
            table_name = config.get('source_table', '').split('.')[-1]
            source_table = config.get('source_table')
            hub_table = config.get('hub_table', '')
            satellite_table = config.get('cur_satellite_table', '')
            bizview_table = config.get('bizview_table', '')
            
            print(f"\n{'=' * 50}")
            print(f"Processing table: {table_name}")
            print(f"Source: {source_table}")
            print(f"Hub: {hub_table}")
            print(f"Satellite: {satellite_table}")
            print(f"Bizview: {bizview_table}")
            print(f"{'=' * 50}")
            
            # Count source records
            try:
                source_count_query = f"SELECT COUNT(*) FROM {source_table}"
                source_count = session.sql(source_count_query).collect()[0][0]
                print(f"Source table total records: {source_count}")
            except Exception as e:
                print(f"ERROR counting source table records for {table_name}: {str(e)}")
                source_count = 0
            
            # Count non-deleted records
            non_deleted_count = source_count
            deleted_count = 0
            if "deleted_column" in config:
                try:
                    deleted_col = config["deleted_column"]
                    non_deleted_query = f"SELECT COUNT(*) FROM {source_table} WHERE {deleted_col} = FALSE"
                    non_deleted_count = session.sql(non_deleted_query).collect()[0][0]
                    deleted_count = source_count - non_deleted_count
                    print(f"Source non-deleted records: {non_deleted_count}")
                    print(f"Deleted records: {deleted_count}")
                except Exception as e:
                    print(f"ERROR processing deleted records for {table_name}: {str(e)}")
            
            # Extract missing records using direct JSON conversion
            source_hub_loss, missing_records = extract_missing_records(session, config)
            
            # Initialize lost records dictionary
            lost_records[table_name] = {
                "source_to_hub": missing_records,
                "missing_count": source_hub_loss,
                "hub_to_satellite": [],
                "hub_to_link": [],
                "link_to_satellite": [],
                "satellite_to_bizview": [],
                "deleted": deleted_count
            }
            
            # Compute metrics
            hub_count = non_deleted_count - source_hub_loss
            
            # Add placeholders for other potential losses
            hub_to_link_loss = 0
            hub_to_sat_loss = 0
            link_to_sat_loss = 0
            sat_to_bizview_loss = 0
            
            # Calculate total rows lost (excluding deleted records)
            total_rows_lost = source_hub_loss + hub_to_link_loss + hub_to_sat_loss + link_to_sat_loss + sat_to_bizview_loss
            
            # Append results
            results.append((
                table_name,
                source_table,
                hub_table,
                satellite_table,
                bizview_table,
                non_deleted_count,
                hub_count,
                hub_count,  # Placeholder for link count
                hub_count,  # Placeholder for satellite count
                hub_count,  # Placeholder for bizview count
                source_hub_loss,
                hub_to_link_loss,
                hub_to_sat_loss,
                link_to_sat_loss,
                sat_to_bizview_loss,
                total_rows_lost,  # Total rows lost
                deleted_count,
                json.dumps(lost_records[table_name], indent=4)
            ))
            
        except Exception as e:
            print(f"CRITICAL ERROR processing {config.get('source_table', 'unknown')}: {str(e)}")
            continue
    
    # Create results DataFrame
    try:
        results_df = session.create_dataframe(results, schema=result_schema)
        
        # Log summary for debugging
        print("\n=== VALIDATION SUMMARY ===")
        for result in results:
            table_name = result[0]
            source_table = result[1]
            hub_table = result[2]
            satellite_table = result[3]
            bizview_table = result[4]
            source_count = result[5]
            total_lost = result[15]   # TOTAL_ROWS_LOST column
            deleted = result[16]      # DELETED_RECORDS column
            
            # Calculate actual sample size (subtract note record if it exists)
            sample_size = len(lost_records[table_name]['source_to_hub'])
            if lost_records[table_name]['missing_count'] > 10:
                # If there's a note record at the end, adjust the count
                sample_size -= 1
            
            print(f"Table: {table_name}")
            print(f"  Source Table: {source_table}")
            print(f"  Hub Table: {hub_table}")
            print(f"  Satellite Table: {satellite_table}")
            print(f"  Bizview Table: {bizview_table}")
            print(f"  Source Count: {source_count}")
            print(f"  Total Rows Lost: {total_lost}")
            print(f"  Deleted: {deleted}")
            print(f"  Lost records sample: {sample_size} records of {lost_records[table_name]['missing_count']} total")
            print("  ---")
        
        return results_df
    except Exception as e:
        print(f"ERROR creating final results dataframe: {str(e)}")
        return session.create_dataframe([], schema=result_schema)
