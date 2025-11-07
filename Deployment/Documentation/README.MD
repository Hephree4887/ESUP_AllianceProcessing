# JSON ↔ MySQL Converter
    A Python-based GUI application designed for SQL engineers to efficiently import JSON files into MySQL databases and export MySQL data to JSON format. This bidirectional tool provides automatic schema inference, transaction safety, and progress tracking—eliminating manual table creation and streamlining data conversion workflows.
## Purpose
    This internal tool automates the bidirectional data conversion process between JSON files and MySQL databases. It can:
        - **Import**: Analyze JSON structure, create appropriate table schemas, and safely import data with full transaction support
        - **Export**: Read from PostScript_AllianceMerge_NEW table and generate batched JSON files in Alliance Community Bridge (ACB) format

## Key Features

### Import Features
    - **Zero Manual Schema Work** - Analyzes JSON and creates tables automatically
    - **Transaction Safety** - Each file is atomic; either fully imports or rolls back completely
    - **Smart Type Inference** - Automatically maps JSON types to optimal MySQL column types
    - **Batch Processing** - Handles multiple JSON files in one operation
### Export Features
    - **Batched Export** - Exports in configurable batches (default 2500 entity groups per file)
    - **ACB Format** - Generates JSON files in Alliance Community Bridge structure
    - **Project-Based Naming** - Files named with your project prefix (e.g., ILKane1.json, ILKane2.json)
    - **Entity Grouping** - Intelligently groups related records by EntityID
### Common Features
    - **Pre-Flight Connection Testing** - Verify credentials before any operation begins
    - **Real-Time Progress Tracking** - Visual progress bar and detailed status logging
    - **Comprehensive Reporting** - Clear summaries of successful and failed operations
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
       5. Click **Browse** and select your working directory
       6. Choose your operation:
          - Click **Import JSON Exception Files from EJ** for imports
          - Click **Export PostScript_AllianceMerge to JSON Files** for exports
       7. Monitor progress and review the summary

**That's it!** No Python, no package installations, no configuration files to edit.

## Installation for Developers
If you need to run from source or modify the tool:
    Simply double-click `run.bat` in the project folder.

** OR **

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

Before any operation can proceed, you must test your connection:

    1. Fill in all five connection fields
    2. Click **Test Connection**
    3. Wait for the green checkmark: "✓ Connection successful"

Both Import and Export buttons remain disabled until:
    - Connection test succeeds
    - A working directory is selected

This prevents accidental operations on wrong databases.

### Selecting Working Directory

Click **Browse** and navigate to your working directory. This directory serves different purposes depending on the operation:

- **For Import**: Source directory containing JSON files to import
- **For Export**: Destination directory where JSON files will be created

The directory can be:
    - A local folder: `C:\Data\JSONFiles`
    - A mapped network drive: `Z:\Imports\JSON`
    - A UNC path: `\\fileserver\share\imports`

## Import Operations

### How Import Works

The import process reads JSON files and creates corresponding MySQL tables:

1. Each JSON filename (without .json extension) becomes the table name
2. The tool analyzes all records to determine optimal column types
3. Tables are created with an auto-increment `id` primary key
4. All records are inserted in a single transaction
5. If anything fails, changes are rolled back

### File Naming Requirements

**CRITICAL:** The filename (without .json extension) becomes the table name.

Examples:
- `customers.json` → Creates table `customers`
- `sales_2024.json` → Creates table `sales_2024`
- `product-catalog.json` → Creates table `product-catalog`

Use valid MySQL table names: alphanumeric characters, underscores, and hyphens only.

### Supported JSON Formats

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

### Data Type Mapping

The tool intelligently maps JSON types to MySQL column types:

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

### Import Table Structure

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

### CRITICAL: Import Drops Existing Tables

**READ THIS CAREFULLY:** This tool will **DROP AND RECREATE** any table with a matching name.

If you have a table called `customers` and import `customers.json`:
1. The existing `customers` table is DROPPED (all data lost)
2. A new `customers` table is created
3. Data from `customers.json` is imported

**This is intentional behavior** for data conversion workflows where you're repeatedly importing fresh exports.

**ALWAYS VERIFY** you're connected to the correct conversion/staging database, not production!

## Export Operations

### How Export Works

The export process reads from the PostScript_AllianceMerge_NEW table and generates JSON files:

1. You provide a project name (e.g., "ILKane")
2. The tool counts total entities in PostScript_AllianceMerge_NEW
3. Records are batched into groups of 2500 entities per file
4. Each batch is exported to a numbered JSON file (ILKane1.json, ILKane2.json, etc.)
5. Output format matches Alliance Community Bridge (ACB) specification

### Export Prerequisites

The export function requires:
- A table named `PostScript_AllianceMerge_NEW` in your database
- The table must contain these columns:
  - EntityID
  - ApplicationID
  - EntityType
  - TargetID
  - SourceIDValue
  - CommunityID

### Executing an Export

1. Test your database connection
2. Select a working directory (where JSON files will be created)
3. Click **Export PostScript_AllianceMerge to JSON Files**
4. Enter your project name in State_County format (e.g., "ILKane")
5. Monitor the progress bar and status window
6. Review the export summary

### Export File Format

The tool generates JSON files in this format:

```json
{
  "CommunityId": "01J426NNSJECDE9K9NTV620TTV",
  "Entities": [
    {
      "Entity": [
        {
          "system": "6BESriNk5FMgbYHfXey23Y",
          "type": "person",
          "applicationId": "20607112"
        },
        {
          "system": "4UPaPj834TL1EQPPDHcuJv",
          "type": "person",
          "applicationId": "6422153"
        }
      ]
    },
    {
      "Entity": [
        {
          "system": "6BESriNk5FMgbYHfXey23Y",
          "type": "case",
          "applicationId": "5391767",
          "correlationId": "5391767-4558830"
        },
        {
          "system": "4UPaPj834TL1EQPPDHcuJv",
          "type": "case",
          "applicationId": "CM-4740850",
          "correlationId": "5391767-4558830"
        }
      ]
    }
  ]
}
```

### Export File Naming

Files are named with your project prefix and a sequential number:
- First file: `[ProjectName]1.json`
- Second file: `[ProjectName]2.json`
- And so on...

Example: Project name "ILKane" produces:
- ILKane1.json
- ILKane2.json
- ILKane3.json
- etc.

### Export Batch Size

By default, each JSON file contains 2500 entity groups. This is configurable in the code but not exposed in the GUI. The batch size balances:
- File size (manageable for downstream processing)
- Number of files generated
- Memory usage during export

## Monitoring Operations

### Progress Bar

The progress bar shows overall completion percentage for both import and export operations:
- Import: Updates after each JSON file is processed
- Export: Updates after each batch file is created

### Status Window

The status window displays detailed logs:
- Connection status messages
- File processing progress
- Success and failure notifications
- Detailed error messages
- Final operation summary

### Understanding Summaries

#### Import Summary Example:
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

#### Export Summary Example:
```
============================================================
EXPORT SUMMARY
============================================================
Total entities processed: 7500
Files created: 3
Output directory: C:\Data\Exports

Files created:
  - ILKane1.json
  - ILKane2.json
  - ILKane3.json
============================================================
```

## Transaction Behavior

### Import Transactions

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

### Export Transactions

Export operations are read-only and do not modify the database. They use a single database connection and cursor throughout the export process.

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

### "No JSON Files Found" (Import)

**Symptoms:** Status window shows "No JSON files found in the selected directory"

**Solutions:**
- Verify directory path is correct
- Ensure files have `.json` extension (not `.txt` or `.json.txt`)
- Check file permissions (can you open them in Notepad?)
- Verify you're looking in the right folder

### "No entities found" (Export)

**Symptoms:** Export completes but reports "No entities found in PostScript_AllianceMerge_NEW table"

**Solutions:**
- Verify the table name is exactly `PostScript_AllianceMerge_NEW`
- Check that the table contains data: `SELECT COUNT(*) FROM PostScript_AllianceMerge_NEW;`
- Verify you're connected to the correct database
- Ensure your user has SELECT permissions on the table

### "Invalid JSON Format" Errors (Import)

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

### Operation Buttons Stay Disabled

**Symptoms:** Cannot click Import or Export buttons

**Requirements:**
1. Connection must be tested successfully (green checkmark)
2. Directory must be selected

**If Test Connection succeeded but buttons still disabled:**
- Make sure you've clicked Browse and selected a folder
- Check that the folder path appears in the Working Directory field

### Slow Performance

**Import:**
- Large JSON files (>100MB)
- Network drive instead of local drive
- Many small files instead of fewer large files
- MySQL server under heavy load

**Export:**
- Large number of entities (>100,000)
- Network drive destination
- MySQL server under heavy load
- Complex entity relationships requiring many JOINs

**What's Normal:**
- Import: ~500-1500 records/second
- Export: ~1000-2000 entities/second

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

1. **Use dedicated accounts** - Don't use root or admin accounts
2. **Grant minimal permissions**:
   - Import: CREATE, DROP, INSERT, SELECT on target database
   - Export: SELECT on PostScript_AllianceMerge_NEW table
3. **Use staging databases** - Never point this at production
4. **Review before operations** - Always test connection first
5. **Audit operations** - Check MySQL logs if needed

## For Developers: Building the Executable

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
   - Test both import and export functionality

4. Deploy:
   - Copy `JSONtoMySQL.exe` to deployment location
   - Update documentation
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

## Known Limitations

### Import Limitations

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

### Export Limitations

1. **Fixed table name**
   - Hardcoded to read from `PostScript_AllianceMerge_NEW`
   - Cannot export from other tables without code modification

2. **Fixed batch size**
   - Default 2500 entities per file
   - Not configurable through GUI

3. **Memory usage**
   - Each batch is loaded into memory
   - Very large entity groups may impact performance

4. **ACB format only**
   - Output format is fixed to Alliance Community Bridge structure
   - Cannot generate other JSON formats

## Best Practices

### Before Import

1. **Validate a sample file** - Test with one file first
2. **Check file names** - Ensure they match desired table names
3. **Verify JSON syntax** - Use online validators for complex files
4. **Backup if needed** - Consider MySQL dump of target database
5. **Check disk space** - Ensure MySQL server has adequate space

### Before Export

1. **Verify source data** - Check PostScript_AllianceMerge_NEW table exists and contains data
2. **Check disk space** - Ensure destination has adequate space for JSON files
3. **Choose project name carefully** - Use State_County format (e.g., ILKane)
4. **Clear destination folder** - Remove old export files if needed

### During Operations

1. **Watch the status window** - Monitor for errors in real-time
2. **Don't close the application** - Let operation complete
3. **Note any failures** - Review error messages

### After Import

1. **Review the summary** - Check success/failure counts
2. **Verify data** - Spot-check imported tables in MySQL
3. **Check row counts** - Ensure expected number of records imported
4. **Validate data types** - Verify columns have appropriate types
5. **Test queries** - Run sample queries to verify data integrity

### After Export

1. **Review the summary** - Check file counts and entity counts
2. **Verify file contents** - Spot-check generated JSON files
3. **Validate JSON syntax** - Ensure files are valid JSON
4. **Check file sizes** - Verify files are reasonably sized
5. **Test downstream processing** - Ensure exported files work in target system

## Typical Workflows

### Import Workflow

```
1. Receive JSON exports from source system
   ↓
2. Place JSON files in accessible directory
   ↓
3. Launch converter tool
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

### Export Workflow

```
1. Prepare PostScript_AllianceMerge_NEW table with entity mappings
   ↓
2. Launch converter tool
   ↓
3. Test connection to database
   ↓
4. Select destination directory
   ↓
5. Execute export with project name
   ↓
6. Monitor progress and review summary
   ↓
7. Verify generated JSON files
   ↓
8. Deliver files to Alliance Community Bridge
```

## Getting Help

### Self-Service

1. **Check this README** - Most questions answered here
2. **Review status window** - Error messages are detailed
3. **Validate JSON files** - Use jsonlint.com
4. **Test in MySQL Workbench** - Verify connection and queries work outside tool

### Contact Support

If you need assistance:
- Email: jeff.reichert@tylertech.com
- Team: ESUP Conversion

**Include in your support request:**
- Screenshot of error message
- Sample JSON file (if not sensitive)
- MySQL version and server details
- Operation you were attempting (import or export)
- What you've already tried

## Technical Reference

### Technology Stack

- **Language:** Python 3.9
- **GUI Framework:** tkinter (built-in)
- **Database Driver:** MySQL Connector/Python 8.4
- **Threading:** Python threading module
- **Packaging:** PyInstaller 6.x

### Performance Characteristics

**Import:**
- **Connection:** Single connection per session
- **Transaction:** One transaction per file
- **Insert method:** Batch `executemany()` for efficiency
- **Memory:** Entire JSON file loaded into RAM
- **Typical speed:** 500-1500 records/second

**Export:**
- **Connection:** Single connection with dictionary cursor
- **Batching:** Configurable batch size (default 2500 entities)
- **Query method:** Parameterized queries with LIMIT/OFFSET
- **Memory:** One batch loaded into RAM at a time
- **Typical speed:** 1000-2000 entities/second

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

### Database Schema Requirements

**For Export:**
The PostScript_AllianceMerge_NEW table must have this structure:

```sql
CREATE TABLE PostScript_AllianceMerge_NEW (
    EntityID BIGINT,
    ApplicationID VARCHAR(255),
    EntityType VARCHAR(255),
    TargetID VARCHAR(255),
    SourceIDValue VARCHAR(255),
    CommunityID VARCHAR(255),
    INDEX idx_entity (EntityID),
    INDEX idx_application (ApplicationID)
);
```

### Dependencies

```
mysql-connector-python==8.4
```

**Note:** tkinter is included with Python by default. If missing, reinstall Python with tkinter support.

## Version History

### Version 3.0 (Current)

**New Features:**
- **Export Functionality** - Export PostScript_AllianceMerge_NEW to JSON files
- **Bidirectional Operations** - Single tool for both import and export
- **ACB Format Support** - Generates Alliance Community Bridge compatible JSON
- **Project-Based Naming** - User-defined project prefixes for exported files
- **Batched Export** - Configurable entity grouping (default 2500 per file)

**Improvements:**
- Updated UI to "JSON ↔ MySQL Converter"
- Unified working directory selection for both operations
- Enhanced status logging for export operations
- Improved error handling for export scenarios

**Bug Fixes:**
- None - new feature release

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

---

## Questions?

**Questions?** Check this README first, then reach out to the Database Engineering team.

**Found a bug?** Please report it with details so we can improve the tool for everyone.

**Have a suggestion?** We're always looking to make this tool better. Let us know what would help your workflow.

---

**Last Updated:** October 29, 2025
**Version:** 3.0
**Maintained By:** ESUP Conversion Team