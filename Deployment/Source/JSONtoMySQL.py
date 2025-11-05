import mysql.connector
import json
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk, simpledialog
from pathlib import Path
import threading
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, date
import os
import csv  

class JSONtoMySQL:
    """
    Handles the business logic for importing JSON files into MySQL.

    This class manages database connections, table creation with automatic 
    schema inference, and data insertion with transaction safety. Each instance 
    represents a single database connection that should be closed when operations 
    are complete.
    """
    
    def __init__(self, host: str, user: str, password: str, database: str, 
                 port: int = 3306, status_callback=None):
        """
        Initialize database connection.
        """
        self.status_callback = status_callback
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            connect_timeout=10,
            autocommit=False  #manage transactions explicitly
        )
        self.cursor = self.connection.cursor()
        self.log("Database connection established")
    
    def log(self, message: str):
        """
        Send status messages to callback if provided, always print to console.
        """
        if self.status_callback:
            self.status_callback(message)
        print(message)
    
    def _determine_column_type(self, values: List[Any]) -> str:
        """
        Determine the most appropriate MySQL column type for a list of values.
        
        This method implements a precedence hierarchy:
        1. JSON - for nested structures (dicts/lists)
        2. TEXT - for long strings or mixed types
        3. VARCHAR(255) - for short strings
        4. DOUBLE - for any floating point
        5. BIGINT - for large integers
        6. INT - for small integers
        7. BOOLEAN - for boolean values
        
        Args:
            values: List of values to analyze (all values for a single column)
        
        Returns:
            MySQL column type as string
        
        Note: We scan ALL values because a column might have mixed types
        in different rows (e.g., some nulls, some integers, some floats).
        We need to pick a type that can accommodate all of them.
        """
        # Filter out None values for type checking
        non_null_values = [v for v in values if v is not None]
        
        if not non_null_values:
            return "TEXT"  # If all values are None, default to TEXT
        
        # Check for nested structures first (highest precedence)
        has_nested = any(isinstance(v, (dict, list)) for v in non_null_values)
        if has_nested:
            return "JSON"
        
        # Check for strings
        has_string = any(isinstance(v, str) for v in non_null_values)
        if has_string:
            # Check if any string is longer than 255 characters
            max_length = max((len(v) for v in non_null_values if isinstance(v, str)), default=0)
            return "TEXT" if max_length > 255 else "VARCHAR(255)"
        
        # Check for floating point numbers
        has_float = any(isinstance(v, float) for v in non_null_values)
        if has_float:
            return "DOUBLE"
        
        # Check for integers (excluding booleans, since in Python bool is a subclass of int)
        integers = [v for v in non_null_values if isinstance(v, int) and not isinstance(v, bool)]
        if integers:
            max_value = max(abs(v) for v in integers)
            return "BIGINT" if max_value >= 2147483648 else "INT"
        
        # Check for booleans
        has_bool = any(isinstance(v, bool) for v in non_null_values)
        if has_bool:
            return "BOOLEAN"
        
        # Default fallback
        return "TEXT"
    
    def create_table_from_json(self, table_name: str, json_data: List[Dict]) -> Tuple[bool, List[str]]:
        """
        Create MySQL table based on JSON data structure.
        
        This method analyzes all records to create a table schema that can
        accommodate all fields across all records (union of all keys).
        
        Args:
            table_name: Name for the new table
            json_data: List of JSON objects (dictionaries)
        
        Returns:
            Tuple of (success: bool, columns: List[str])
            - success: True if table was created, False if skipped
            - columns: Ordered list of column names for use in INSERT statements
        
        Important: This function DROPS the existing table if it exists.
        This is intentional behavior for data conversion/import workflows.
        """
        if not json_data:
            self.log(f"No data in {table_name}.json - skipping")
            return False, []

        # Step 1: Collect all unique keys from all records
        # In SQL terms, this is like doing a UNION of all possible columns
        all_keys = set()
        for record in json_data:
            all_keys.update(record.keys())
        
        # Sort keys for deterministic, consistent table structure
        sorted_columns = sorted(list(all_keys))
        self.log(f"Columns to be created for {table_name}: {sorted_columns}")

        # Step 2: Determine appropriate MySQL type for each column
        column_types = {}
        for key in sorted_columns:
            # Gather all values for this key across all records
            values = [record.get(key) for record in json_data]
            column_types[key] = self._determine_column_type(values)

        # Step 3: Build CREATE TABLE statement
        # Every table gets an auto-increment primary key named 'id'
        columns_sql = ["id BIGINT AUTO_INCREMENT PRIMARY KEY"]
        columns_sql.extend([f"`{key}` {column_types[key]}" for key in sorted_columns])

        # Drop existing table (this is intentional - see function docstring)
        drop_sql = f"DROP TABLE IF EXISTS `{table_name}`"
        self.cursor.execute(drop_sql)
        self.log(f"Dropped table {table_name} if it existed")

        # Create the new table
        create_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns_sql)})"
        self.cursor.execute(create_sql)
        
        # Don't commit yet - we'll commit after data insertion succeeds
        self.log(f"Created table {table_name} with {len(sorted_columns)} columns")
        
        # Return success and the column order for INSERT statements
        return True, sorted_columns
    
    def insert_json_data(self, table_name: str, json_data: List[Dict], columns: List[str]):
        """
        Insert JSON records into the specified table.
        
        Args:
            table_name: Target table name
            json_data: List of JSON objects to insert
            columns: Ordered list of column names (from create_table_from_json)
        
        Note: This uses parameterized queries (%s placeholders) which prevents
        SQL injection attacks. It's like using sp_executesql with parameters in SQL Server.
        """
        if not json_data:
            return

        # Build INSERT statement with proper column names and placeholders
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'`{col}`' for col in columns])
        insert_sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"

        # Prepare all rows for batch insertion
        # Using None for missing fields - MySQL will insert NULL
        values = []
        for record in json_data:
            row = tuple(record.get(col) for col in columns)
            values.append(row)

        # Execute batch insert - more efficient than inserting one row at a time
        self.cursor.executemany(insert_sql, values)
        self.log(f"Inserted {len(values)} records into {table_name}")
    
    def import_json_file(self, json_file_path: str) -> Tuple[bool, str]:
        """
        Import a single JSON file into a MySQL table.
        
        This method wraps the entire import process in a transaction.
        If anything fails, all changes are rolled back automatically.
        
        Args:
            json_file_path: Full path to JSON file
        
        Returns:
            Tuple of (success: bool, message: str)
        
        Transaction behavior: Each file import is atomic - either the entire
        file imports successfully, or nothing is changed in the database.
        This is similar to wrapping operations in BEGIN TRAN...COMMIT/ROLLBACK.
        """
        table_name = Path(json_file_path).stem
        
        try:
            # Read and parse JSON file
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Handle empty files or empty arrays
            if not json_data or (isinstance(json_data, list) and len(json_data) == 0):
                msg = f"Skipped {json_file_path} - File is empty or contains no data"
                self.log(msg)
                return False, msg
            
            # Normalize to list format (convert single object to list)
            if isinstance(json_data, dict):
                json_data = [json_data]
            
            # Create table and get column order
            success, columns = self.create_table_from_json(table_name, json_data)
            
            if not success:
                return False, f"Failed to create table for {json_file_path}"
            
            # Insert data using the correct column order
            self.insert_json_data(table_name, json_data, columns)
            
            # Commit the transaction - this makes all changes permanent
            self.connection.commit()
            
            success_msg = f"Successfully imported {json_file_path} ({len(json_data)} records)"
            self.log(success_msg)
            return True, success_msg
            
        except json.JSONDecodeError as e:
            # Roll back any partial changes
            self.connection.rollback()
            error_msg = f"Skipped {json_file_path} - Invalid JSON format: {str(e)}"
            self.log(error_msg)
            return False, error_msg
            
        except Exception as e:
            # Roll back any partial changes
            self.connection.rollback()
            error_msg = f"ERROR importing {json_file_path}: {str(e)}"
            self.log(error_msg)
            return False, error_msg
    
    def import_directory(self, directory_path: str) -> Dict[str, Any]:
        """
        Import all JSON files from a directory.
        
        Args:
            directory_path: Path to directory containing JSON files
        
        Returns:
            Dictionary containing summary statistics:
            {
                'total': int,
                'successful': int,
                'failed': int,
                'success_files': List[str],
                'failed_files': List[str]
            }
        """
        json_files = list(Path(directory_path).glob('*.json'))
        
        if not json_files:
            self.log("No JSON files found in the selected directory")
            return {
                'total': 0,
                'successful': 0,
                'failed': 0,
                'success_files': [],
                'failed_files': []
            }
        
        self.log(f"\nFound {len(json_files)} JSON file(s) to import\n")
        
        # Track results for summary
        successful_imports = []
        failed_imports = []
        
        for json_file in json_files:
            success, message = self.import_json_file(str(json_file))
            
            if success:
                successful_imports.append(json_file.name)
            else:
                failed_imports.append(json_file.name)
        
        # Log summary
        self.log("\n" + "="*60)
        self.log("IMPORT SUMMARY")
        self.log("="*60)
        self.log(f"Total files processed: {len(json_files)}")
        self.log(f"Successfully imported: {len(successful_imports)}")
        self.log(f"Failed imports: {len(failed_imports)}")
        
        if failed_imports:
            self.log("\nFailed files:")
            for filename in failed_imports:
                self.log(f"  - {filename}")
        
        self.log("="*60)
        
        return {
            'total': len(json_files),
            'successful': len(successful_imports),
            'failed': len(failed_imports),
            'success_files': successful_imports,
            'failed_files': failed_imports
        }
    
    def close(self):
        """Close database connection and clean up resources."""
        self.cursor.close()
        self.connection.close()
        self.log("Database connection closed")
    
    def __enter__(self):
        """Context manager support - enables 'with' statement usage."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support - ensures connection is closed."""
        self.close()
class MySQLToJSON:
    """
    Handles the business logic for exporting MySQL PostScript_AllianceMerge_NEW table to JSON files.

    This class reads from the PostScript_AllianceMerge_NEW table, groups records by EntityID,
    and exports them in batches of 2500 entity groups per JSON file. The output format
    matches the Alliance Community Bridge (ACB) JSON structure with nested entities.
    """
    
    def __init__(self, host: str, user: str, password: str, database: str,
                 port: int = 3306, status_callback=None):
        """
        Initialize database connection for export operations.
        
        Args:
            host: MySQL server hostname
            user: MySQL username
            password: MySQL password
            database: Database name containing PostScript_AllianceMerge_NEW table
            port: MySQL port (default 3306)
            status_callback: Optional callback function for status messages
        """
        self.status_callback = status_callback
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            connect_timeout=10,
            autocommit=False
        )
        self.cursor = self.connection.cursor(dictionary=True)
        self.log("Database connection established for export")
    
    def log(self, message: str):
        """
        Send status messages to callback if provided, always print to console.
        """
        if self.status_callback:
            self.status_callback(message)
        print(message)
    
    def get_entity_count(self) -> int:
        """
        Get the count of distinct EntityIDs in the PostScript_AllianceMerge_NEW table.
        
        Returns:
            Integer count of unique entities
        """
        query = "SELECT COUNT(DISTINCT EntityID) as entity_count FROM PostScript_AllianceMerge_NEW"
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result['entity_count'] if result else 0
    
    def get_entity_id_range(self, offset: int, limit: int) -> List[int]:
        """
        Get a batch of EntityIDs for processing.
        
        This query gets a specific range of distinct EntityIDs, which we'll then use
        to fetch all records associated with those entities. This is how we batch
        the export into files of 2500 entity groups.
        
        Args:
            offset: Starting position in the sorted list of EntityIDs
            limit: Number of EntityIDs to retrieve
        
        Returns:
            List of EntityID values
        """
        query = """
            SELECT DISTINCT EntityID 
            FROM PostScript_AllianceMerge_NEW 
            ORDER BY EntityID 
            LIMIT %s OFFSET %s
        """
        self.cursor.execute(query, (limit, offset))
        results = self.cursor.fetchall()
        return [row['EntityID'] for row in results]
    
    def get_records_for_entities(self, entity_ids: List[int]) -> List[Dict]:
        """
        Fetch all records for a given list of EntityIDs.
        
        Args:
            entity_ids: List of EntityID values to fetch
        
        Returns:
            List of dictionaries containing the record data
        """
        if not entity_ids:
            return []
        
        # Build the IN clause with proper parameterization
        placeholders = ','.join(['%s'] * len(entity_ids))
        query = f"""
            SELECT EntityID, ApplicationID, EntityType, TargetID, SourceIDValue, CommunityID
            FROM PostScript_AllianceMerge_NEW
            WHERE EntityID IN ({placeholders})
            ORDER BY EntityID, ApplicationID
        """
        
        self.cursor.execute(query, entity_ids)
        return self.cursor.fetchall()
    
    def build_json_structure(self, records: List[Dict]) -> Dict:
        """
        Transform flat database records into the nested JSON structure required by ACB.
        
        The structure is:
        {
            "CommunityId": "...",
            "Entities": [
                {
                    "Entity": [
                        {"system": "...", "type": "...", "applicationId": "...", "correlationId": "..."},
                        ...
                    ]
                },
                ...
            ]
        }
        
        Args:
            records: List of flat database records
        
        Returns:
            Dictionary representing the complete JSON structure
        """
        if not records:
            return {}
        
        # Get CommunityID from first record (should be same for all records in batch)
        community_id = records[0]['CommunityID']
        
        # Group records by EntityID
        # This creates a dictionary where keys are EntityIDs and values are lists of records
        entities_by_id = {}
        for record in records:
            entity_id = record['EntityID']
            if entity_id not in entities_by_id:
                entities_by_id[entity_id] = []
            entities_by_id[entity_id].append(record)
        
        # Build the Entities array
        entities_array = []
        for entity_id in sorted(entities_by_id.keys()):
            entity_records = entities_by_id[entity_id]
            
            # Build the Entity array for this EntityID
            # Each record becomes one item in the Entity array
            entity_array = []
            for record in entity_records:
                entity_item = {
                    "system": record['ApplicationID'],
                    "type": record['EntityType'],
                    "applicationId": record['TargetID']
                }
                
                # Only include correlationId if SourceIDValue has a value
                # This matches the pattern in Sample1.json where some entities have it and others don't
                if record['SourceIDValue']:
                    entity_item['correlationId'] = record['SourceIDValue']
                
                entity_array.append(entity_item)
            
            # Add this entity group to the Entities array
            entities_array.append({"Entity": entity_array})
        
        # Build the complete structure
        return {
            "CommunityId": community_id,
            "Entities": entities_array
        }
    
    def export_to_json_files(self, output_directory: str, file_prefix: str,
                            batch_size: int = 2500) -> Dict[str, Any]:
        """
        Export PostScript_AllianceMerge_NEW table to batched JSON files.
        
        This is the main export method that orchestrates the entire process:
        1. Count total entities
        2. Calculate number of batch files needed
        3. For each batch:
           - Fetch the EntityIDs for this batch
           - Fetch all records for those EntityIDs
           - Build the nested JSON structure
           - Write to file
        
        Args:
            output_directory: Directory path where JSON files will be written
            file_prefix: Filename prefix (e.g., "ILKane" results in "ILKane1.json")
            batch_size: Number of entity groups per file (default 2500)
        
        Returns:
            Dictionary with export statistics
        """
        try:
            # Ensure output directory exists
            os.makedirs(output_directory, exist_ok=True)
            
            # Get total entity count
            total_entities = self.get_entity_count()
            
            if total_entities == 0:
                self.log("No entities found in PostScript_AllianceMerge_NEW table")
                return {
                    'total_entities': 0,
                    'files_created': 0,
                    'success': False,
                    'error': 'No data to export'
                }
            
            self.log(f"Found {total_entities} entities to export")
            
            # Calculate number of files needed
            num_files = (total_entities + batch_size - 1) // batch_size
            self.log(f"Will create {num_files} JSON file(s) with {batch_size} entities each")
            
            files_created = []
            
            # Process each batch
            for file_num in range(1, num_files + 1):
                self.log(f"\nProcessing batch {file_num} of {num_files}...")
                
                # Calculate offset for this batch
                offset = (file_num - 1) * batch_size
                
                # Get the EntityIDs for this batch
                entity_ids = self.get_entity_id_range(offset, batch_size)
                self.log(f"Retrieved {len(entity_ids)} entity IDs for batch {file_num}")
                
                # Get all records for these EntityIDs
                records = self.get_records_for_entities(entity_ids)
                self.log(f"Retrieved {len(records)} total records for batch {file_num}")
                
                # Build the JSON structure
                json_structure = self.build_json_structure(records)
                
                # Write to file
                filename = f"{file_prefix}{file_num}.json"
                filepath = os.path.join(output_directory, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(json_structure, f, indent=2, ensure_ascii=False)
                
                files_created.append(filename)
                self.log(f"Created file: {filename} with {len(json_structure.get('Entities', []))} entities")
            
            # Log summary
            self.log("\n" + "="*60)
            self.log("EXPORT SUMMARY")
            self.log("="*60)
            self.log(f"Total entities processed: {total_entities}")
            self.log(f"Files created: {len(files_created)}")
            self.log(f"Output directory: {output_directory}")
            self.log("\nFiles created:")
            for filename in files_created:
                self.log(f"  - {filename}")
            self.log("="*60)
            
            return {
                'total_entities': total_entities,
                'files_created': len(files_created),
                'filenames': files_created,
                'success': True
            }
            
        except Exception as e:
            error_msg = f"ERROR during export: {str(e)}"
            self.log(error_msg)
            return {
                'total_entities': 0,
                'files_created': 0,
                'success': False,
                'error': str(e)
            }
    
    def close(self):
        """Close database connection and clean up resources."""
        self.cursor.close()
        self.connection.close()
        self.log("Database connection closed")
    
    def __enter__(self):
        """Context manager support - enables 'with' statement usage."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support - ensures connection is closed."""
        self.close()
class TmpAllianceExporter:
    """
    Handles exporting the tmp_Alliance mapping table to both CSV and JSON formats.
    
    This class is specifically designed for the fixed-schema tmp_Alliance table which
    contains entity mappings between legacy systems and the Alliance Community systems.
    It generates two output files in a single operation to ensure data consistency.
    """
    
    # Define the column structure once - we know this is fixed
    COLUMNS = [
        'SourceIDValue', 'TargetID', 'EntityType', 'ApplicationID', 'ClientID',
        'TimeStampCreate', 'PushNumber', 'SourceDBName', 'SourceTableName',
        'SourceColumnName', 'NameFirst', 'NameLast', 'NameMid', 'NameSuffix',
        'BirthDate', 'ReferralNumber'
    ]
    
    def __init__(self, host: str, user: str, password: str, database: str,
                 port: int = 3306, status_callback=None):
        """
        Initialize database connection for tmp_Alliance export.
        
        Since we know the exact structure of tmp_Alliance, we can optimize
        our queries and processing for this specific table layout.
        """
        self.status_callback = status_callback
        self.database_name = database  # Store for validation queries
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            connect_timeout=10,
            autocommit=False
        )
        # Use dictionary cursor for cleaner field access
        self.cursor = self.connection.cursor(dictionary=True)
        self.log("Database connection established for tmp_Alliance export")
    
    def log(self, message: str):
        """Send status messages to callback and console."""
        if self.status_callback:
            self.status_callback(message)
        print(message)
    def validate_table_structure(self) -> Tuple[bool, str]:
        """
        Verify that tmp_Alliance exists and has the expected structure.
        
        This validation step ensures we're working with the correct table
        schema before attempting to export data. It's a safety check that
        prevents runtime errors if the table structure has been modified.
        """
        try:
            # Check if table exists
            self.cursor.execute("""
                SELECT COUNT(*) as table_exists 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = 'tmp_Alliance'
            """, (self.database_name,))
            
            result = self.cursor.fetchone()
            if not result['table_exists']:
                return False, "Table tmp_Alliance does not exist in the database"
            
            # Verify column structure
            self.cursor.execute("""
                SELECT COLUMN_NAME 
                FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = 'tmp_Alliance'
                ORDER BY ORDINAL_POSITION
            """, (self.database_name,))
            
            actual_columns = [row['COLUMN_NAME'] for row in self.cursor.fetchall()]
            
            # Check if all expected columns are present
            missing_columns = set(self.COLUMNS) - set(actual_columns)
            if missing_columns:
                return False, f"Missing expected columns: {', '.join(missing_columns)}"
            
            return True, "Table structure validated successfully"
            
        except Exception as e:
            return False, f"Error validating table structure: {str(e)}"
    def get_record_count(self) -> int:
        """Get total number of records in tmp_Alliance for progress tracking."""
        self.cursor.execute("SELECT COUNT(*) as count FROM tmp_Alliance")
        result = self.cursor.fetchone()
        return result['count'] if result else 0
    def fetch_data_in_batches(self, batch_size: int = 10000) -> List[Dict]:
        """
        Fetch all data from tmp_Alliance in memory-efficient batches.
        
        For large tables, this approach prevents memory overflow by processing
        data in chunks. The batch size of 10000 is a good balance between
        memory usage and query efficiency.
        """
        all_data = []
        offset = 0
        
        # Build the SELECT query with proper column ordering
        column_list = ', '.join([f'`{col}`' for col in self.COLUMNS])
        query = f"""
            SELECT {column_list}
            FROM tmp_Alliance
            ORDER BY PushNumber, EntityType, SourceIDValue
            LIMIT %s OFFSET %s
        """
        
        while True:
            self.cursor.execute(query, (batch_size, offset))
            batch = self.cursor.fetchall()
            
            if not batch:
                break
            
            all_data.extend(batch)
            offset += batch_size
            
            self.log(f"Fetched {len(all_data)} records so far...")
        
        return all_data
    def _format_datetime_for_export(self, dt_value) -> str:
        """
        Format datetime values consistently for export.
        
        MySQL datetime objects need to be converted to strings for JSON
        and CSV export. We use ISO format for consistency and compatibility.
        """
        if dt_value is None:
            return ""
        elif isinstance(dt_value, datetime):
            return dt_value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(dt_value, date):
            return dt_value.strftime('%Y-%m-%d')
        else:
            return str(dt_value)
    def _export_to_csv(self, data: List[Dict], filepath: str) -> bool:
        """
        Export data to CSV format with proper encoding and formatting.
        
        This method handles special characters, NULL values, and ensures
        compatibility with Excel and other spreadsheet applications.
        """
        try:
            import csv
            
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                # utf-8-sig adds BOM for Excel compatibility
                writer = csv.DictWriter(csvfile, fieldnames=self.COLUMNS)
                
                # Write header row
                writer.writeheader()
                
                # Process each row
                for row in data:
                    # Format datetime fields
                    formatted_row = {}
                    for key, value in row.items():
                        if key in ['TimeStampCreate', 'BirthDate']:
                            formatted_row[key] = self._format_datetime_for_export(value)
                        elif value is None:
                            formatted_row[key] = ''  # Empty string for NULL values
                        else:
                            formatted_row[key] = value
                    
                    writer.writerow(formatted_row)
            
            self.log(f"Successfully exported {len(data)} records to CSV: {filepath}")
            return True
            
        except Exception as e:
            self.log(f"Error exporting to CSV: {str(e)}")
            return False
    def _export_to_json(self, data: List[Dict], filepath: str) -> bool:
        """
        Export data to JSON format with proper type handling.
        
        The JSON output is formatted as an array of objects, making it easy
        to import into other systems or process with standard JSON tools.
        """
        try:
            # Prepare data for JSON serialization
            json_data = []
            
            for row in data:
                json_row = {}
                for key, value in row.items():
                    if key in ['TimeStampCreate', 'BirthDate']:
                        # Convert datetime/date objects to strings
                        json_row[key] = self._format_datetime_for_export(value)
                    elif value is None:
                        json_row[key] = None  # Preserve NULL as null in JSON
                    else:
                        json_row[key] = value
                
                json_data.append(json_row)
            
            # Write to file with pretty formatting
            with open(filepath, 'w', encoding='utf-8') as jsonfile:
                json.dump(json_data, jsonfile, indent=2, ensure_ascii=False)
            
            self.log(f"Successfully exported {len(data)} records to JSON: {filepath}")
            return True
            
        except Exception as e:
            self.log(f"Error exporting to JSON: {str(e)}")
            return False
    def export_to_files(self, output_directory: str, project_name: str) -> Dict[str, Any]:
        """
        Main export method that creates both CSV and JSON files.
        
        This method orchestrates the entire export process:
        1. Validates the table structure
        2. Fetches all data from tmp_Alliance
        3. Exports to both CSV and JSON formats
        4. Provides detailed success/failure reporting
        
        The atomic nature ensures both files are created successfully
        or neither is created, maintaining consistency.
        """
        try:
            # Ensure output directory exists
            os.makedirs(output_directory, exist_ok=True)
            
            # Validate table structure first
            valid, message = self.validate_table_structure()
            if not valid:
                self.log(f"Validation failed: {message}")
                return {
                    'success': False,
                    'error': message,
                    'total_records': 0
                }
            
            # Get record count for progress tracking
            total_records = self.get_record_count()
            
            if total_records == 0:
                self.log("No records found in tmp_Alliance table")
                return {
                    'success': False,
                    'error': 'No data to export',
                    'total_records': 0
                }
            
            self.log(f"Found {total_records} records to export")
            
            # Fetch all data
            self.log("Fetching data from tmp_Alliance...")
            data = self.fetch_data_in_batches()
            
            # Generate file paths
            csv_filename = f"{project_name}_Alliance.csv"
            json_filename = f"{project_name}_Alliance.json"
            csv_filepath = os.path.join(output_directory, csv_filename)
            json_filepath = os.path.join(output_directory, json_filename)
            
            # Use temporary files for atomic operation
            temp_csv = csv_filepath + '.tmp'
            temp_json = json_filepath + '.tmp'
            
            # Export to temporary files
            self.log("Creating CSV file...")
            csv_success = self._export_to_csv(data, temp_csv)
            
            if not csv_success:
                # Clean up temporary file if it exists
                if os.path.exists(temp_csv):
                    os.remove(temp_csv)
                return {
                    'success': False,
                    'error': 'Failed to create CSV file',
                    'total_records': total_records
                }
            
            self.log("Creating JSON file...")
            json_success = self._export_to_json(data, temp_json)
            
            if not json_success:
                # Clean up temporary files
                if os.path.exists(temp_csv):
                    os.remove(temp_csv)
                if os.path.exists(temp_json):
                    os.remove(temp_json)
                return {
                    'success': False,
                    'error': 'Failed to create JSON file',
                    'total_records': total_records
                }
            
            # Both exports successful - rename temp files to final names
            os.rename(temp_csv, csv_filepath)
            os.rename(temp_json, json_filepath)
            
            # Log summary
            self.log("\n" + "="*60)
            self.log("EXPORT SUMMARY")
            self.log("="*60)
            self.log(f"Total records exported: {total_records}")
            self.log(f"Files created:")
            self.log(f"  - CSV: {csv_filename}")
            self.log(f"  - JSON: {json_filename}")
            self.log(f"Output directory: {output_directory}")
            self.log("="*60)
            
            return {
                'success': True,
                'total_records': total_records,
                'files_created': [csv_filename, json_filename],
                'csv_file': csv_filepath,
                'json_file': json_filepath
            }
            
        except Exception as e:
            error_msg = f"Unexpected error during export: {str(e)}"
            self.log(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'total_records': 0
            }
    def close(self):
        """Close database connection and clean up resources."""
        self.cursor.close()
        self.connection.close()
        self.log("Database connection closed")
    
    def __enter__(self):
        """Context manager support."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()
class ImporterGUI:
    """
    Graphical user interface for the JSON to MySQL importer and MySQL to JSON exporter.
    
    This class handles all UI interactions and delegates business logic
    to the JSONtoMySQL and MySQLToJSON classes. It follows the Model-View pattern where
    those classes are the Models and this class is the View.
    """
    # Configuration file for saving connection settings
    CONFIG_FILE = "importer_config.json"
    
    def __init__(self, root):
        """Initialize the GUI components."""
        self.root = root
        self.root.title("JSON ↔ MySQL Converter")
        self.root.geometry("650x900")
        self.root.resizable(False, False)
        
        # Connection state tracking
        self.connection_verified = False
        
        # Create all GUI components
        self.create_connection_frame()
        self.create_test_connection_button()
        self.create_directory_frame()
        self.create_progress_bar()
        self.create_execute_button()
        self.create_status_window()
        
        # Load saved configuration
        self.load_config()
        
        # Initial state - disable import and export buttons
        self.update_button_states()
    def create_connection_frame(self):
        """Create database connection input fields."""
        frame = tk.LabelFrame(self.root, text="Database Connection", padx=10, pady=10)
        frame.pack(padx=10, pady=10, fill="x")
        
        # Host
        tk.Label(frame, text="Host:", width=10, anchor="w").grid(row=0, column=0, sticky="w", pady=5)
        self.host_entry = tk.Entry(frame, width=40)
        self.host_entry.grid(row=0, column=1, pady=5)
        self.host_entry.insert(0, "db-gov-central.mgmt.cms.caseloadpro.com")
        # Bind to reset connection state when changed
        self.host_entry.bind('<KeyRelease>', self.on_connection_field_changed)
        
        # Port
        tk.Label(frame, text="Port:", width=10, anchor="w").grid(row=1, column=0, sticky="w", pady=5)
        self.port_entry = tk.Entry(frame, width=40)
        self.port_entry.grid(row=1, column=1, pady=5)
        self.port_entry.insert(0, "3306")
        self.port_entry.bind('<KeyRelease>', self.on_connection_field_changed)
        
        # User
        tk.Label(frame, text="Username:", width=10, anchor="w").grid(row=2, column=0, sticky="w", pady=5)
        self.user_entry = tk.Entry(frame, width=40)
        self.user_entry.grid(row=2, column=1, pady=5)
        self.user_entry.bind('<KeyRelease>', self.on_connection_field_changed)
        
        # Password
        tk.Label(frame, text="Password:", width=10, anchor="w").grid(row=3, column=0, sticky="w", pady=5)
        self.password_entry = tk.Entry(frame, width=40, show="*")
        self.password_entry.grid(row=3, column=1, pady=5)
        self.password_entry.bind('<KeyRelease>', self.on_connection_field_changed)
        
        # Database
        tk.Label(frame, text="Database:", width=10, anchor="w").grid(row=4, column=0, sticky="w", pady=5)
        self.database_entry = tk.Entry(frame, width=40)
        self.database_entry.grid(row=4, column=1, pady=5)
        self.database_entry.bind('<KeyRelease>', self.on_connection_field_changed)
    def create_test_connection_button(self):
        """Create test connection button."""
        frame = tk.Frame(self.root)
        frame.pack(padx=10, pady=5, fill="x")
        
        self.test_conn_btn = tk.Button(
            frame,
            text="Test Connection",
            command=self.test_connection,
            bg="#2196F3",
            fg="white",
            font=("Arial", 10, "bold"),
            height=1
        )
        self.test_conn_btn.pack(pady=5)
        
        # Connection status label
        self.conn_status_label = tk.Label(
            frame,
            text="Connection not tested",
            font=("Arial", 9),
            fg="gray"
        )
        self.conn_status_label.pack(pady=2)
    def create_directory_frame(self):
        """Create directory selection - used for both import source and export destination."""
        frame = tk.LabelFrame(self.root, text="Working Directory", padx=10, pady=10)
        frame.pack(padx=10, pady=10, fill="x")
        
        self.directory_var = tk.StringVar()
        self.directory_var.trace('w', lambda *args: self.update_button_states())
        
        tk.Entry(frame, textvariable=self.directory_var, width=50, state="readonly").pack(side="left", padx=5)
        tk.Button(frame, text="Browse...", command=self.browse_directory, width=10).pack(side="left")
    def create_progress_bar(self):
        """Create progress bar for import/export operations."""
        frame = tk.Frame(self.root)
        frame.pack(padx=10, pady=5, fill="x")
        
        tk.Label(frame, text="Progress:", font=("Arial", 9)).pack(anchor="w")
        
        self.progress_bar = ttk.Progressbar(
            frame,
            orient="horizontal",
            length=600,
            mode="determinate"
        )
        self.progress_bar.pack(fill="x", pady=5)
        self.progress_bar["value"] = 0
    def create_execute_button(self):
        """Create import and export buttons."""
        # Export tmp_Alliance button
        self.tmp_alliance_btn = tk.Button(
            self.root,
            text="Scenario 1: Export tmp_Alliance to CSV and JSON",
            command=self.execute_tmp_alliance_export,
            bg="#FF9800",  # Orange color to distinguish it
            fg="white",
            font=("Arial", 12, "bold"),
            height=2,
            state="disabled"
        )
        self.tmp_alliance_btn.pack(padx=10, pady=(0,10), fill="x")
        # Export button
        self.export_btn = tk.Button(
            self.root,
            text="Scenario 2,3: Export Matched Entities to JSON Files",
            command=self.execute_export,
            bg="#1500ff",
            fg="white",
            font=("Arial", 12, "bold"),
            height=2,
            state="disabled"  # Initially disabled
        )
        self.export_btn.pack(padx=10, pady=(0,10), fill="x")
        # Import button
        self.execute_btn = tk.Button(
            self.root,
            text="Scenario 1,2,3,4: Import JSON Files from EJ",
            command=self.execute_import,
            bg="#4CAF50",
            fg="white",
            font=("Arial", 12, "bold"),
            height=2,
            state="disabled"  # Initially disabled
        )
        self.execute_btn.pack(padx=10, pady=(10,5), fill="x")

    def create_status_window(self):
        """Create status output window."""
        frame = tk.LabelFrame(self.root, text="Status", padx=10, pady=10)
        frame.pack(padx=10, pady=10, fill="both", expand=True)
        
        self.status_text = scrolledtext.ScrolledText(frame, height=12, state="disabled", wrap="word")
        self.status_text.pack(fill="both", expand=True)
    def on_connection_field_changed(self, event=None):
        """Reset connection verification when connection fields change."""
        self.connection_verified = False
        self.conn_status_label.config(text="Connection not tested", fg="gray")
        self.update_button_states()
    def update_button_states(self):
        """Enable/disable all operation buttons based on prerequisites."""
        if self.connection_verified and self.directory_var.get().strip():
            self.execute_btn.config(state="normal")
            self.export_btn.config(state="normal")
            self.tmp_alliance_btn.config(state="normal")  # Add this line
        else:
            self.execute_btn.config(state="disabled")
            self.export_btn.config(state="disabled")
            self.tmp_alliance_btn.config(state="disabled")  # Add this line    
    def test_connection(self):
        """
        Test database connection with provided credentials.
        
        This runs in a separate thread to prevent UI freezing during
        the connection attempt.
        """
        if not self.validate_connection_inputs():
            return
        
        # Disable button during test
        self.test_conn_btn.config(state="disabled")
        self.conn_status_label.config(text="Testing connection...", fg="orange")
        
        # Run in thread to prevent UI blocking
        thread = threading.Thread(target=self.run_connection_test)
        thread.start()
    def run_connection_test(self):
        """Execute the connection test."""
        try:
            # Attempt to connect
            test_conn = mysql.connector.connect(
                host=self.host_entry.get().strip(),
                user=self.user_entry.get().strip(),
                password=self.password_entry.get().strip(),
                database=self.database_entry.get().strip(),
                port=int(self.port_entry.get().strip()),
                connect_timeout=10
            )
            test_conn.close()
            
            # Success
            self.connection_verified = True
            self.conn_status_label.config(text="✓ Connection successful", fg="green")
            messagebox.showinfo("Success", "Database connection successful!")
            
            # Save successful connection settings
            self.save_config()
            
        except mysql.connector.Error as err:
            self.connection_verified = False
            error_msg = f"Connection failed: {err}"
            self.conn_status_label.config(text="✗ Connection failed", fg="red")
            messagebox.showerror("Connection Error", error_msg)
            
        except ValueError:
            self.connection_verified = False
            self.conn_status_label.config(text="✗ Invalid port number", fg="red")
            messagebox.showerror("Validation Error", "Port must be a valid number")
            
        except Exception as e:
            self.connection_verified = False
            error_msg = f"Unexpected error: {str(e)}"
            self.conn_status_label.config(text="✗ Connection failed", fg="red")
            messagebox.showerror("Error", error_msg)
            
        finally:
            # Re-enable button
            self.test_conn_btn.config(state="normal")
            self.update_button_states()
    def browse_directory(self):
        """Open directory browser dialog."""
        directory = filedialog.askdirectory(title="Select Working Directory")
        if directory:
            self.directory_var.set(directory)
    def log_status(self, message: str):
        """Add message to status window."""
        self.status_text.config(state="normal")
        self.status_text.insert("end", message + "\n")
        self.status_text.see("end")
        self.status_text.config(state="disabled")
        self.root.update_idletasks()
    def validate_connection_inputs(self):
        """Validate connection input fields."""
        if not self.host_entry.get().strip():
            messagebox.showerror("Validation Error", "Host is required")
            return False
        
        if not self.port_entry.get().strip():
            messagebox.showerror("Validation Error", "Port is required")
            return False
        
        try:
            port = int(self.port_entry.get().strip())
            if port < 1 or port > 65535:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Validation Error", "Port must be a number between 1 and 65535")
            return False
        
        if not self.user_entry.get().strip():
            messagebox.showerror("Validation Error", "Username is required")
            return False
        
        if not self.password_entry.get().strip():
            messagebox.showerror("Validation Error", "Password is required")
            return False
        
        if not self.database_entry.get().strip():
            messagebox.showerror("Validation Error", "Database name is required")
            return False
        
        return True
    def validate_import_inputs(self):
        """Validate inputs before import execution."""
        if not self.connection_verified:
            messagebox.showerror("Validation Error", "Please test the database connection first")
            return False
        
        if not self.directory_var.get().strip():
            messagebox.showerror("Validation Error", "Working directory is required")
            return False
        
        return True
    def validate_export_inputs(self):
        """Validate inputs before export execution."""
        if not self.connection_verified:
            messagebox.showerror("Validation Error", "Please test the database connection first")
            return False
        
        if not self.directory_var.get().strip():
            messagebox.showerror("Validation Error", "Working directory is required")
            return False
        
        return True
    def execute_import(self):
        """Execute the import process."""
        if not self.validate_import_inputs():
            return
        
        # Disable buttons during import
        self.execute_btn.config(state="disabled")
        self.export_btn.config(state="disabled")
        self.tmp_alliance_btn.config(state="disabled")
        self.test_conn_btn.config(state="disabled")
        
        # Clear status window and reset progress bar
        self.status_text.config(state="normal")
        self.status_text.delete(1.0, "end")
        self.status_text.config(state="disabled")
        self.progress_bar["value"] = 0
        
        # Run import in separate thread to prevent UI freezing
        thread = threading.Thread(target=self.run_import)
        thread.start()
    def execute_export(self):
        """Execute the export process."""
        if not self.validate_export_inputs():
            return
        
        # Prompt user for project name
        project_name = simpledialog.askstring(
            "Project Name",
            "Please enter your project name in State_County format\n(e.g., ILKane):",
            parent=self.root
        )
        
        if not project_name:
            # User cancelled
            return
        
        # Basic validation of project name
        project_name = project_name.strip()
        if not project_name:
            messagebox.showerror("Validation Error", "Project name cannot be empty")
            return
        
        # Disable buttons during export
        self.execute_btn.config(state="disabled")
        self.export_btn.config(state="disabled")
        self.tmp_alliance_btn.config(state="disabled")
        self.test_conn_btn.config(state="disabled")
        
        # Clear status window and reset progress bar
        self.status_text.config(state="normal")
        self.status_text.delete(1.0, "end")
        self.status_text.config(state="disabled")
        self.progress_bar["value"] = 0
        
        # Run export in separate thread to prevent UI freezing
        thread = threading.Thread(target=self.run_export, args=(project_name,))
        thread.start()
    def run_import(self):
        """
        Run the import process with progress tracking.
    
        This method runs in a background thread, so we need to be careful
        about updating the UI (must use update_idletasks).
        """
        try:
            self.log_status("Starting import process...\n")
        
            # Create importer instance with callback
            importer = JSONtoMySQL(
                host=self.host_entry.get().strip(),
                user=self.user_entry.get().strip(),
                password=self.password_entry.get().strip(),
                database=self.database_entry.get().strip(),
                port=int(self.port_entry.get().strip()),
                status_callback=self.log_status
            )
        
            # Get list of files for progress tracking
            directory = self.directory_var.get().strip()
            json_files = list(Path(directory).glob('*.json'))
            total_files = len(json_files)
        
            if total_files == 0:
                self.log_status("No JSON files found in the selected directory")
                importer.close()
                return
        
            self.log_status(f"Found {total_files} JSON file(s) to import\n")
        
            # Track results manually since we're not using import_directory()
            successful_imports = []
            failed_imports = []
        
            # Import with progress updates
            for idx, json_file in enumerate(json_files, 1):
                success, message = importer.import_json_file(str(json_file))
            
                # Track results
                if success:
                    successful_imports.append(json_file.name)
                else:
                    failed_imports.append(json_file.name)
            
                # Update progress bar
                progress = (idx / total_files) * 100
                self.progress_bar["value"] = progress
                self.root.update_idletasks()
        
            # Display summary manually (mimicking what import_directory() does)
            self.log_status("\n" + "="*60)
            self.log_status("IMPORT SUMMARY")
            self.log_status("="*60)
            self.log_status(f"Total files processed: {total_files}")
            self.log_status(f"Successfully imported: {len(successful_imports)}")
            self.log_status(f"Failed imports: {len(failed_imports)}")
        
            if failed_imports:
                self.log_status("\nFailed files:")
                for filename in failed_imports:
                    self.log_status(f"  - {filename}")
        
            self.log_status("="*60)
        
            importer.close()
        
            # Complete progress bar
            self.progress_bar["value"] = 100
        
            messagebox.showinfo("Success", "Import process completed!\nCheck status window for details.")
        
        except mysql.connector.Error as err:
            error_msg = f"Database Error: {err}"
            self.log_status(f"\nERROR: {error_msg}")
            messagebox.showerror("Database Error", error_msg)
        
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            self.log_status(f"\nERROR: {error_msg}")
            messagebox.showerror("Error", error_msg)
        
        finally:
            # Re-enable buttons
            self.execute_btn.config(state="normal")
            self.export_btn.config(state="normal")
            self.tmp_alliance_btn.config(state="normal")
            self.test_conn_btn.config(state="normal")
    def run_export(self, project_name: str):
        """
        Run the export process with progress tracking.
        
        This method runs in a background thread, so we need to be careful
        about updating the UI (must use update_idletasks).
        
        Args:
            project_name: The project name prefix for output files
        """
        try:
            self.log_status("Starting export process...\n")
            self.log_status(f"Project name: {project_name}\n")
        
            # Create exporter instance with callback
            exporter = MySQLToJSON(
                host=self.host_entry.get().strip(),
                user=self.user_entry.get().strip(),
                password=self.password_entry.get().strip(),
                database=self.database_entry.get().strip(),
                port=int(self.port_entry.get().strip()),
                status_callback=self.log_status
            )
        
            # Get output directory
            output_directory = self.directory_var.get().strip()
        
            # Get total entity count for progress tracking
            total_entities = exporter.get_entity_count()
            
            if total_entities == 0:
                self.log_status("No entities found in PostScript_AllianceMerge_NEW table")
                exporter.close()
                messagebox.showwarning("No Data", "No entities found to export")
                return
            
            # Calculate number of files that will be created
            batch_size = 2500
            num_files = (total_entities + batch_size - 1) // batch_size
            
            # Export with progress updates
            # We'll update progress as files are created
            result = exporter.export_to_json_files(
                output_directory=output_directory,
                file_prefix=project_name,
                batch_size=batch_size
            )
        
            exporter.close()
        
            # Complete progress bar
            self.progress_bar["value"] = 100
        
            if result['success']:
                messagebox.showinfo(
                    "Success", 
                    f"Export completed successfully!\n\n"
                    f"Files created: {result['files_created']}\n"
                    f"Total entities: {result['total_entities']}\n\n"
                    f"Check status window for details."
                )
            else:
                messagebox.showerror(
                    "Export Failed",
                    f"Export failed: {result.get('error', 'Unknown error')}"
                )
        
        except mysql.connector.Error as err:
            error_msg = f"Database Error: {err}"
            self.log_status(f"\nERROR: {error_msg}")
            messagebox.showerror("Database Error", error_msg)
        
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            self.log_status(f"\nERROR: {error_msg}")
            messagebox.showerror("Error", error_msg)
        
        finally:
            # Re-enable buttons
            self.execute_btn.config(state="normal")
            self.export_btn.config(state="normal")
            self.tmp_alliance_btn.config(state="normal")
            self.test_conn_btn.config(state="normal")
    def execute_tmp_alliance_export(self):
        """Execute the tmp_Alliance export process."""
        if not self.validate_export_inputs():  # Reuse existing validation
            return
    
        # Prompt for project name
        project_name = simpledialog.askstring(
            "Project Name",
            "Enter project name for tmp_Alliance export files\n(e.g., ILKane):",
            parent=self.root
        )
    
        if not project_name:
            return
    
        project_name = project_name.strip()
        if not project_name:
            messagebox.showerror("Validation Error", "Project name cannot be empty")
            return
    
        # Disable buttons during export
        self.execute_btn.config(state="disabled")
        self.export_btn.config(state="disabled")
        self.tmp_alliance_btn.config(state="disabled")
        self.test_conn_btn.config(state="disabled")
    
        # Clear status and reset progress
        self.status_text.config(state="normal")
        self.status_text.delete(1.0, "end")
        self.status_text.config(state="disabled")
        self.progress_bar["value"] = 0
    
        # Run in separate thread
        thread = threading.Thread(
            target=self.run_tmp_alliance_export,
            args=(project_name,)
        )
        thread.start()
    def run_tmp_alliance_export(self, project_name: str):
        """Run the tmp_Alliance export in a background thread."""
        try:
            self.log_status("Starting tmp_Alliance export process...\n")
            self.log_status(f"Project name: {project_name}\n")
        
            # Create exporter with the necessary import
            from datetime import datetime, date  # Add at top of method
            import os  # Add at top of method
        
            exporter = TmpAllianceExporter(
                host=self.host_entry.get().strip(),
                user=self.user_entry.get().strip(),
                password=self.password_entry.get().strip(),
                database=self.database_entry.get().strip(),
                port=int(self.port_entry.get().strip()),
                status_callback=self.log_status
            )
        
            # Get output directory
            output_directory = self.directory_var.get().strip()
        
            # Update progress: Starting
            self.progress_bar["value"] = 10
            self.root.update_idletasks()
        
            # Execute export
            result = exporter.export_to_files(output_directory, project_name)
        
            # Update progress: Complete
            self.progress_bar["value"] = 100
            self.root.update_idletasks()
        
            exporter.close()
        
            if result['success']:
                messagebox.showinfo(
                    "Success",
                    f"Export completed successfully!\n\n"
                    f"Records exported: {result['total_records']}\n"
                    f"Files created:\n"
                    f"  • {result['files_created'][0]}\n"
                    f"  • {result['files_created'][1]}\n\n"
                    f"Check status window for details."
                )
            else:
                messagebox.showerror(
                    "Export Failed",
                    f"Export failed: {result.get('error', 'Unknown error')}"
                )
    
        except mysql.connector.Error as err:
            error_msg = f"Database Error: {err}"
            self.log_status(f"\nERROR: {error_msg}")
            messagebox.showerror("Database Error", error_msg)
    
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            self.log_status(f"\nERROR: {error_msg}")
            messagebox.showerror("Error", error_msg)
    
        finally:
            # Re-enable buttons
            self.execute_btn.config(state="normal")
            self.export_btn.config(state="normal")
            self.tmp_alliance_btn.config(state="normal")
            self.test_conn_btn.config(state="normal")
    def save_config(self):
        """Save host and port to configuration file."""
        try:
            config = {
                'host': self.host_entry.get().strip(),
                'port': self.port_entry.get().strip()
            }
            with open(self.CONFIG_FILE, 'w') as f:
                json.dump(config, f)
        except Exception as e:
            print(f"Could not save configuration: {e}")
    def load_config(self):
        """Load host and port from configuration file if it exists."""
        try:
            if Path(self.CONFIG_FILE).exists():
                with open(self.CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    
                # Only update if we have saved values
                if 'host' in config and config['host']:
                    self.host_entry.delete(0, 'end')
                    self.host_entry.insert(0, config['host'])
                    
                if 'port' in config and config['port']:
                    self.port_entry.delete(0, 'end')
                    self.port_entry.insert(0, config['port'])
        except Exception as e:
            print(f"Could not load configuration: {e}")


if __name__ == "__main__":
    root = tk.Tk()
    app = ImporterGUI(root)
    root.mainloop()
