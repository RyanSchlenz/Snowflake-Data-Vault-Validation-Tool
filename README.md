# Snowflake-Data-Vault-Validation-Tool

## Overview

This tool validates data integrity across a Data Vault implementation in Snowflake by comparing source tables with hub, satellite, and business view tables. It identifies records that failed to migrate properly between layers, calculates data loss metrics, and provides detailed reports for troubleshooting.

## Features

- **Cross-Layer Validation**: Compares data across the entire Data Vault architecture (source → hub → satellite → business view)
- **Missing Record Detection**: Uses SQL EXCEPT queries to identify records that failed to migrate between layers
- **Detailed Record Samples**: Provides JSON samples of missing records with metadata for troubleshooting
- **Deleted Record Tracking**: Distinguishes between properly deleted records and actual data loss
- **Customizable Table Mappings**: Easily adaptable to different Data Vault implementations and naming conventions

## Prerequisites

- Snowflake account with access to both source and Data Vault databases
- Snowpark for Python
- Python 3.8+
- Required packages:
  - `snowflake-snowpark-python`
  - `json`

## Setup

1. Install required Python packages:
   ```bash
   pip install snowflake-snowpark-python
   ```

2. Configure your Snowflake connection parameters in your environment or a configuration file.

3. Customize the `table_configs` list in the script to match your Data Vault architecture.

## Configuration

The script is driven by a `table_configs` list containing dictionaries that define the mapping between your source tables and Data Vault structures. For each entity, you'll need to configure:

```python
{
    "source_table": "SOURCE_DB.SOURCE_SCHEMA.ENTITY_TABLE",    # Source system table
    "hub_table": "DV_DB.RAWVAULT.H_ENTITY",                   # Hub table
    "cur_satellite_table": "DV_DB.RAWVAULT.S_ENTITY_LROC_INFO_CURRENT",  # Current satellite table
    "bizview_table": "DV_DB.BIZVIEWS.FACT_ENTITY",            # Business view
    "source_key": "ID",                                       # Primary key in source
    "hub_key": "ENTITY_ID",                                  # Business key in hub
    "satellite_hash_key": "HK_H_ENTITY",                     # Hash key in satellite
    "bizview_key": "ENTITY_ID",                              # Business key in bizview
    "deleted_column": "_IS_DELETED",                         # Deleted flag column
    "columns_to_compare": [                                  # Columns to validate
        "NAME", "CODE", "STATUS", "ACTIVE_DATE", ...
    ],
    "custom_except_query": """                               # Custom SQL for detailed validation
    SELECT ID, NAME, CODE, STATUS, ...
    FROM SOURCE_DB.SOURCE_SCHEMA.ENTITY_TABLE
    EXCEPT
    SELECT E.ENTITY_ID, NAME, CODE, STATUS, ...
    FROM DV_DB.RAWVAULT.H_ENTITY E
    JOIN DV_DB.RAWVAULT.S_ENTITY_LROC_INFO SE ON E.HK_H_ENTITY = SE.HK_H_ENTITY
    """
}
```

### Link Table Configuration

For link entities (representing relationships), the configuration needs additional parameters:

```python
{
    "source_table": "SOURCE_DB.SOURCE_SCHEMA.RELATIONSHIP_TABLE",
    "hub_tables": ["DV_DB.RAWVAULT.H_ENTITY_1", "DV_DB.RAWVAULT.H_ENTITY_2"],
    "link_table": "DV_DB.RAWVAULT.L_ENTITY_1_ENTITY_2",
    "cur_satellite_table": "DV_DB.RAWVAULT.S_RELATIONSHIP_LROC_INFO_CURRENT",
    "bizview_table": "DV_DB.BIZVIEWS.FACT_RELATIONSHIP",
    "source_key": "ID",
    "hub_keys": ["ENTITY_1_ID", "ENTITY_2_ID"],
    "link_hash_keys": ["HK_H_ENTITY_1", "HK_H_ENTITY_2"],
    "bizview_key": "RELATIONSHIP_ID",
    ...
}
```

## Usage

### As Snowflake Stored Procedure

1. Create a stored procedure in your Snowflake account:

```sql
CREATE OR REPLACE PROCEDURE RUN_DATA_VAULT_VALIDATION()
RETURNS TABLE (...)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'validation_script.main'
AS
$$
-- Paste the entire script here
$$;
```

2. Execute the procedure:

```sql
CALL RUN_DATA_VAULT_VALIDATION();
```

### As Python Script

```python
# Example usage in a Python script
from snowflake.snowpark import Session
import validation_script

# Create a Snowflake session
connection_parameters = {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "role": "your_role",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
}

session = Session.builder.configs(connection_parameters).create()

# Run the validation
results = validation_script.main(session)

# Display results
results.show()
```

## Output Format

The script produces a DataFrame with the following columns:

| Column                    | Description                                  |
|---------------------------|----------------------------------------------|
| TABLE_NAME                | The entity name                              |
| SOURCE_TABLE              | Full path to source table                    |
| HUB_TABLE                 | Full path to hub table                       |
| SATELLITE_TABLE           | Full path to satellite table                 |
| BIZVIEW_TABLE             | Full path to business view                   |
| SOURCE_COUNT              | Count of non-deleted records in source       |
| HUB_COUNT                 | Count of records in hub table                |
| LINK_COUNT                | Count of records in link table (if applicable) |
| CURRENT_SATELLITE_COUNT   | Count of records in current satellite        |
| BIZVIEW_COUNT             | Count of records in business view            |
| SOURCE_TO_HUB_LOSS        | Records lost between source and hub          |
| HUB_TO_LINK_LOSS          | Records lost between hub and link            |
| HUB_TO_SAT_LOSS           | Records lost between hub and satellite       |
| LINK_TO_SAT_LOSS          | Records lost between link and satellite      |
| SAT_TO_BIZVIEW_LOSS       | Records lost between satellite and bizview   |
| TOTAL_ROWS_LOST           | Total data loss                              |
| DELETED_RECORDS           | Count of properly deleted records            |
| LOST_RECORDS_DETAILS      | JSON with sample records lost                |

## How It Works

The validation process follows these steps:

1. For each table configuration:
   - Count total records in the source table
   - Identify and count logically deleted records
   - Execute custom EXCEPT query to find records missing between source and hub
   - Convert results to JSON for easier analysis
   - Compute metrics for data loss at each layer
   - Capture sample records for detailed investigation

2. Compile all results into a comprehensive report DataFrame

## Best Practices

- Run the validation after each ETL load to catch issues early
- Configure appropriate sample sizes for detailed record inspection (default is 10)
- Include all critical business columns in the comparison
- Customize EXCEPT queries for entity-specific validation logic
- Set up alerting based on threshold percentages of data loss

## Extending the Tool

- Add validation for historical satellites by comparing effective dates
- Implement alerting based on threshold percentages of data loss
- Add scheduled execution for regular validation
- Extend with reconciliation logic for specific business rules
- Add historical trending of data quality metrics

## Troubleshooting

If you encounter issues:

1. Verify database access permissions for all tables
2. Check that hash key generation is consistent across the Data Vault
3. Validate that soft-delete logic is consistent
4. Ensure custom EXCEPT queries have matching column data types
5. For large tables, consider adding sampling or partitioning logic

## License

This tool is provided under the [MIT License](LICENSE).
