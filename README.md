# SQLiteDatabaseComparator

SQLite Database Comparator is a Python script designed to compare two SQLite database files (.db3 format). The script checks for differences in schema, key constraints, data counts, and actual data across the two databases. It's a useful tool for database migration, testing, or auditing purposes.

## Features

- **Schema Comparison**: Compares the schema of the two databases, identifying differences in table and column definitions.
- **Key Constraints Comparison**: Checks and compares primary keys, foreign keys, and indexes between the databases.
- **Data Count Comparison**: Compares the number of rows in each table between the two databases.
- **Data Comparison**: Compares the actual data within tables, highlighting differences row by row.
- **Configurable Output**: Limits the number of differences shown in data comparison to avoid overwhelming output.
- **File Validation**: Ensures that the files provided are valid `.db3` SQLite files before proceeding with the comparison.

## Usage

```bash
python db_comparator.py <db1_path> <db2_path>
```

- `<db1_path>`: Path to the first database file.
- `<db2_path>`: Path to the second database file.

## How It Works

1. **File Validation**: Checks if both files have the `.db3` extension. If not, the script exits with an appropriate message.
2. **Schema Comparison**: The script retrieves and compares the schema from both databases, including table names and column structures.
3. **Key Constraints Comparison**: Primary keys, foreign keys, and indexes are retrieved and compared for all tables in both databases.
4. **Data Count Comparison**: For tables present in both databases, the script compares the number of rows.
5. **Data Comparison**: The script compares the actual data in the tables, highlighting differences in rows up to a configurable limit.
6. **Result Output**: Displays differences found during comparison, with clear indications of schema, key constraints, and data mismatches.

This tool is particularly useful for developers and database administrators who need to ensure consistency between different versions of SQLite databases.