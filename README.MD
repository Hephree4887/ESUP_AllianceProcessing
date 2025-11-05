# JSON to MySQL Importer
    A Python-based GUI application designed for SQL engineers to efficiently import JSON files into MySQL conversion databases. This tool provides automatic schema inference, transaction safety, and progress tracking—eliminating manual table creation and reducing import errors.
## Purpose
    This internal tool automates the tedious process of importing JSON data exports into MySQL conversion servers. It automatically analyzes JSON structure, creates appropriate table schemas, and safely imports data with full transaction support. Built by SQL engineers for SQL engineers.
## Key Features
    - **Zero Manual Schema Work** - Analyzes JSON and creates tables automatically
    - **Transaction Safety** - Each file is atomic; either fully imports or rolls back completely
    - **Pre-Flight Connection Testing** - Verify credentials before any import begins
    - **Real-Time Progress Tracking** - Visual progress bar and detailed status logging
    - **Smart Type Inference** - Automatically maps JSON types to optimal MySQL column types
    - **Comprehensive Error Reporting** - Clear summaries of successful and failed imports
    - **Configuration Memory** - Remembers host and port between sessions
    - **Network Drive Compatible** - Works seamlessly with UNC paths
    - **No External Dependencies for End Users** - Standalone executable requires no Python installation

## System Requirements

    ### For End Users (Executable)
        - Windows 7 or newer
        - Network access to MySQL server
        - No Python installation required

    ### For Developers (Source)
        - Python 3.6 or higher
        - MySQL Connector/Python 8.4
        - Network access to MySQL server

## Quick Start for Users

    ### Running the Executable (Recommended)

        1. Navigate to the deployment location: `\\[your-network-share]\tools\JSONtoMySQL\Executable`
        2. Double-click `JSONtoMySQL.exe`
        3. Enter your database credentials
        4. Click **Test Connection** and wait for success confirmation
        5. Click **Browse** and select your JSON files directory
        6. Click **Execute Import**
        7. Monitor progress and review the summary

**That's it!** No Python, no package installations, no configuration files to edit.

## Installation for Developers

    If you need to run from source or modify the tool:

        ### 1. Verify Python Installation
            ```bash
            python --version
            ```
            
            You should see Python 3.6 or higher. If not, download from [python.org](https://www.python.org/downloads/) and check "Add Python to PATH" during installation.

        ### 2. Install Required Packages

            **IMPORTANT:** You must use version 8.4 of mysql-connector-python. Newer versions (9.x) have compatibility issues with PyInstaller.
        
            ```bash
            pip install mysql-connector-python==8.4
            ```
            
        ### 3. Run the Application

            ```bash
            python JSONtoMySQL.py
            ```

Or simply double-click `run.bat` in the project folder.

### Running Tests

To execute the automated test suite (including the `JSONtoMySQL` column type inference tests), run:

```bash
pytest
```

## Detailed Usage Guide

### Connection Setup

When you launch the application, you'll see five connection fields:

    - **Host**: MySQL server hostname (e.g., `db-gov-central.mgmt.cms.caseloadpro.com`)
    - **Port**: MySQL port (usually `3306`)
    - **Username**: Your MySQL username
    - **Password**: Your MySQL password (never saved)
    - **Database**: Target database name (never saved)

**Security Note:** Only the host and port are saved locally. Credentials are never persisted to disk.

### Testing Your Connection

Before any import can proceed, you must test your connection:

    1. Fill in all five connection fields
    2. Click **Test Connection**
    3. Wait for the green checkmark: "✓ Connection successful"

The Import button remains disabled until both:
    - Connection test succeeds
    - JSON directory is selected

This prevents accidental imports to wrong databases.

### Selecting JSON Files

Click **Browse** and navigate to your JSON files folder. This can be:
    - A local folder: `C:\Data\JSONFiles`
    - A mapped network drive: `Z:\Imports\JSON`
    - A UNC path: `\\fileserver\share\imports`

The tool will process all `.json` files in the selected directory.

### Monitoring the Import

Once you click **Execute Import**:
    - The progress bar shows overall completion percentage
    - The status window displays detailed logs for each file
    - Successful files are noted in green text
    - Failed files show error details
    - The import summary shows final statistics

### Understanding the Summary

After import completes, you'll see something like:

```
============================================================
IMPORT SUMMARY
============================================================
Total files processed: 42
Successfully imported: 40
Failed imports: 2

Failed files:
  - malformed_data.json
  - empty_file.json
============================================================
```

This tells you exactly which files succeeded and which need attention.

## JSON File Requirements

### File Naming

**CRITICAL:** The filename (without .json extension) becomes the table name.

Examples:
    - `customers.json` → Creates table `customers`
    - `sales_2024.json` → Creates table `sales_2024`
    - `product-catalog.json` → Creates table `product-catalog`

Use valid MySQL table names: alphanumeric characters, underscores, and hyphens only.

### Supported JSON Formats

The tool accepts two formats:

**Single Object** (creates one-row table):
    ```json
    {
        "id": 1001,
        "name": "John Doe",
        "email": "john@example.com",
        "active": true,
        "balance": 2500.50
    }
    ```

**Array of Objects** (creates multi-row table):
    ```json
    [
        {
            "id": 1001,
            "name": "John Doe",
            "active": true
        },
        {
            "id": 1002,
            "name": "Jane Smith",
            "active": false
        }
    ]
    ```

**Varying Fields Across Records**:

The tool handles records with different fields. Missing fields become NULL:

    ```json
    [
        {
            "id": 1,
            "name": "Product A",
            "price": 99.99
        },
        {
            "id": 2,
            "name": "Product B",
            "price": 149.99,
            "discount": 10,
            "featured": true
        }
    ]
    ```

Results in a table with columns: `id`, `name`, `price`, `discount`, `featured` (with NULLs where data is missing).

### Files That Will Be Skipped

    - Empty files
    - Files with empty arrays `[]`
    - Files with invalid JSON syntax
    - Files without `.json` extension

Skipped files are logged in the status window with reasons.

## How It Works

### Behind the Scenes Process

When you import a JSON file, here's what happens:

**1. Schema Analysis**
    - Opens and parses the JSON file
    - Identifies all unique field names across all records
    - Samples all values to determine appropriate data types

**2. Table Creation**
    - Drops existing table if it exists (intentional for conversion workflows)
    - Creates table with inferred schema
    - Adds auto-increment `id` PRIMARY KEY as first column

**3. Data Insertion**
    - Uses parameterized queries (SQL injection safe)
    - Batch inserts all records efficiently
    - Handles NULL values for missing fields

**4. Transaction Commit**
    - If everything succeeds: COMMIT transaction
    - If any error occurs: ROLLBACK all changes
    - Each file is atomic: all-or-nothing

### Data Type Mapping

The tool intelligently maps JSON types to MySQL column types:
    |------------------|-------------------|-------------------------------|
    | JSON Value Type  | MySQL Column Type | Selection Criteria            |
    |------------------|-------------------|-------------------------------|
    | Nested obj/array | `JSON`            | Any dict or list value        |
    | Long text        | `TEXT`            | Strings > 255 characters      |
    | Short text       | `VARCHAR(255)`    | Strings ≤ 255 characters      |
    | Decimal          | `DOUBLE`          | Any number with decimal point |
    | Large integer    | `BIGINT`          | Integers ≥ 2,147,483,648      |
    | Integer          | `INT`             | Integers < 2,147,483,648      |
    | Boolean          | `BOOLEAN`         | `true` or `false`             |
    | null             | `TEXT`            | When all values are null      |
    |------------------|-------------------|-------------------------------|
### Table Structure

Every created table includes:
    - **First column:** `id` (BIGINT AUTO_INCREMENT PRIMARY KEY)
    - **Subsequent columns:** Data columns in alphabetical order by field name

Example: `customers.json` becomes:

    ```sql
    CREATE TABLE `customers` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        active BOOLEAN,
        email VARCHAR(255),
        name VARCHAR(255),
        ...
    );
    ```

## Critical Behaviors to Understand

### This Tool DROPS TABLES

**READ THIS CAREFULLY:** This tool will **DROP AND RECREATE** any table with a matching name.

If you have a table called `customers` and import `customers.json`:
    1. The existing `customers` table is DROPPED (all data lost)
    2. A new `customers` table is created
    3. Data from `customers.json` is imported

**This is intentional behavior** for data conversion workflows where you're repeatedly importing fresh exports.

** ALWAYS VERIFY** you're connected to the correct conversion/staging database, not production!

### Transaction Behavior

Each JSON file import is wrapped in a database transaction:

    ```sql
    START TRANSACTION;
        DROP TABLE IF EXISTS `table_name`;
        CREATE TABLE `table_name` (...);
        INSERT INTO `table_name` VALUES (...);
    COMMIT;  -- or ROLLBACK if any error
    ```

This means:
    - ## If import succeeds: table is created and populated
    - ## If import fails: NO table is created, database unchanged
    - ## Partial imports are impossible
    - ## Database always remains in a consistent state

### File Independence

Each JSON file is imported independently:
    - One file's failure doesn't affect others
    - Files are processed sequentially
    - Each gets its own transaction
    - Summary shows which succeeded and which failed

## Troubleshooting

### "Connection Failed" Error

**Symptoms:** Red X with connection error message

**Common Causes:**
    - Incorrect hostname or port
    - Wrong username or password
    - Database doesn't exist
    - Network connectivity issues
    - MySQL server firewall blocking connection

**Solutions:**
    1. Verify credentials by testing in MySQL Workbench or command line
    2. Ensure database exists: `SHOW DATABASES;`
    3. Check network: `ping your-mysql-server.com`
    4. Verify port: `netstat -an | findstr 3306`
    5. Check MySQL user permissions: `SHOW GRANTS FOR 'username'@'%';`

### "No JSON Files Found"

**Symptoms:** Status window shows "No JSON files found in the selected directory"

**Solutions:**
    - Verify directory path is correct
    - Ensure files have `.json` extension (not `.txt` or `.json.txt`)
    - Check file permissions (can you open them in Notepad?)
    - Verify you're looking in the right folder

### "Invalid JSON Format" Errors

**Symptoms:** Files are skipped with "Invalid JSON format" message

**Solutions:**
    1. Validate JSON syntax at [jsonlint.com](https://jsonlint.com)
    2. Check for:
       - Missing commas between fields
       - Trailing commas after last field
       - Unquoted keys
       - Single quotes instead of double quotes
       - Special characters not properly escaped
    3. Verify file encoding is UTF-8
    4. Open file in text editor and look for corruption

### Import Button Stays Disabled

**Symptoms:** Cannot click Execute Import button

**Requirements:**
    1. Connection must be tested successfully (green checkmark)
    2. Directory must be selected

**If Test Connection succeeded but button still disabled:**
    - Make sure you've clicked Browse and selected a folder
    - Check that the folder path appears in the JSON Files Location field

### Slow Performance

**Symptoms:** Imports taking longer than expected

**Common Causes:**
    - Large JSON files (>100MB)
    - Network drive instead of local drive
    - Many small files instead of fewer large files
    - MySQL server under heavy load

**What's Normal:**
    - Local files: ~1000 records/second
    - Network files: ~500 records/second
    - The tool is not designed for massive datasets (>1GB)

## Security and Privacy

### What Gets Saved

**Saved locally** (in `importer_config.json`):
    - Host
    - Port

**Never saved:**
    - Username
    - Password
    - Database name

### Credential Handling

    - Credentials are validated once during Connection Test
    - Stored only in memory during the session
    - Cleared when application closes
    - Never written to disk, logs, or config files

### Network Security

    - All database connections use MySQL's standard authentication
    - Connections use whatever security MySQL server requires (SSL if configured)
    - Tool respects MySQL's user permission system

### Recommended Practices

    1. **Use dedicated import accounts** - Don't use root or admin accounts
    2. **Grant minimal permissions** - Only need: CREATE, DROP, INSERT, SELECT on target database
    3. **Use staging databases** - Never point this at production
    4. **Review before importing** - Always test connection first
    5. **Audit imports** - Check MySQL logs if needed

## 📝 For Developers: Building the Executable

If you need to rebuild the executable after making code changes:

### Prerequisites

    ```bash
    pip install pyinstaller
    pip install mysql-connector-python==8.4
    ```

**CRITICAL:** Use version 8.4. Version 9.x has issues with PyInstaller that cause authentication failures.

### Build Command

    ```bash
    pyinstaller --onefile --windowed --name "JSONtoMySQL" --collect-all mysql.connector JSONtoMySQL.py
    ```

**Flag Explanation:**
    - `--onefile`: Creates single .exe file
    - `--windowed`: Hides console window (GUI only)
    - `--name "JSONtoMySQL"`: Names the output file
    - `--collect-all mysql.connector`: **ESSENTIAL** - Includes all MySQL authentication plugins

### Build Process

1. Clean previous builds:
       ```bash
       rmdir /s build
       rmdir /s dist
       ```

2. Run PyInstaller command above

3. Test the executable:
       - Navigate to `dist` folder
       - Run `JSONtoMySQL.exe`
       - Test connection and sample import

4. Deploy:
       - Copy `JSONtoMySQL.exe` to deployment location
       - Update README if needed
       - Notify team of new version

### Troubleshooting Build Issues

**"Authentication plugin module could not be found" error:**
    - Ensure you're using version 8.4 of mysql-connector-python
    - Verify the `--collect-all mysql.connector` flag is present
    - Clean build folders and rebuild from scratch

**Executable won't run:**
    - Check if antivirus is blocking it
    - Try running as administrator
    - Verify Windows is 64-bit (tool builds for 64-bit)

## 🐛 Known Limitations

1. **Table names must be valid MySQL identifiers**
   - Use alphanumeric, underscores, hyphens only
   - Avoid MySQL reserved words

2. **Memory usage for large files**
   - Entire JSON file is loaded into memory
   - Files >1GB may cause performance issues
   - Consider splitting very large files

3. **Column name case sensitivity**
   - Preserves case from JSON keys
   - `Name` and `name` become different columns

4. **No append mode**
   - Always drops and recreates tables
   - Cannot add to existing data

5. **Nested JSON as JSON type**
   - Requires MySQL 5.7.8 or newer
   - Stored as JSON strings, not expanded

## Best Practices

### Before Importing

    1. **Validate a sample file** - Test with one file first
    2. **Check file names** - Ensure they match desired table names
    3. **Verify JSON syntax** - Use online validators for complex files
    4. **Backup if needed** - Consider MySQL dump of target database
    5. **Check disk space** - Ensure MySQL server has adequate space

### During Import

    1. **Watch the status window** - Monitor for errors in real-time
    2. **Don't close the application** - Let import complete
    3. **Note failed files** - Review error messages for failures

### After Import

    1. **Review the summary** - Check success/failure counts
    2. **Verify data** - Spot-check imported tables in MySQL
    3. **Check row counts** - Ensure expected number of records imported
    4. **Validate data types** - Verify columns have appropriate types
    5. **Test queries** - Run sample queries to verify data integrity

## Typical Workflow

Here's how this tool fits into a typical data conversion process:

    ```
    1. Receive JSON exports from source system
       ↓
    2. Place JSON files in accessible directory
       ↓
    3. Launch importer tool
       ↓
    4. Test connection to conversion database
       ↓
    5. Select JSON directory
       ↓
    6. Execute import
       ↓
    7. Monitor progress and review summary
       ↓
    8. Verify data in MySQL Workbench
       ↓
    9. Proceed with data transformation/migration
    ```

## Getting Help

### Self-Service

    1. **Check this README** - Most questions answered here
    2. **Review status window** - Error messages are detailed
    3. **Validate JSON files** - Use jsonlint.com
    4. **Test in MySQL Workbench** - Verify connection works outside tool

### Contact Support

If you need assistance:
    - Email: [jeff.reichert@company.com]
    - Team: ESUP Conversion 

**Include in your support request:**
    - Screenshot of error message
    - Sample JSON file (if not sensitive)
    - MySQL version and server details
    - What you've already tried

## Technical Reference

### Technology Stack

    - **Language:** Python 3.9
    - **GUI Framework:** tkinter (built-in)
    - **Database Driver:** MySQL Connector/Python 8.4
    - **Threading:** Python threading module
    - **Packaging:** PyInstaller 6.x

### Performance Characteristics

    - **Connection:** Single connection per session
    - **Transaction:** One transaction per file
    - **Insert method:** Batch `executemany()` for efficiency
    - **Memory:** Entire JSON file loaded into RAM
    - **Typical speed:** 500-1500 records/second (depends on network)

### File Structure

    ```
        JSONtoMySQL/
        ├── JSONtoMySQL.py          # Main application
        ├── requirements.txt        # Python dependencies
        ├── README.MD              # This file
        ├── run.bat                # Windows launcher
        ├── importer_config.json   # Auto-generated config (host/port only)
        └── dist/
            └── JSONtoMySQL.exe    # Compiled executable
    ```

### Dependencies

    ````
        mysql-connector-python==8.4
    ````

**Note:** tkinter is included with Python by default. If missing, reinstall Python with tkinter support.

## Version History

### Version 2.0 (October 7, 2025)

**New Features:**
    - Connection testing before import
    - Real-time progress tracking with progress bar
    - Comprehensive import summaries
    - Configuration persistence (host/port)
    - Enhanced error handling and logging
    - Transaction safety for atomic imports

**Improvements:**
    - Better type inference algorithm
    - Improved column ordering (alphabetical)
    - Clearer error messages
    - Faster batch insertions

**Bug Fixes:**
    - Fixed authentication plugin issues with PyInstaller
    - Resolved library conflicts
    - Corrected column ordering inconsistencies

## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## 
    **Questions?** Check this README first, then reach out to the Database Engineering team.
    **Found a bug?** Please report it with details so we can improve the tool for everyone.
    **Have a suggestion?** We're always looking to make this tool better. Let us know what would help your workflow.
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## 
---
