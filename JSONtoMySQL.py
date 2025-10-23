import mysql.connector
import json
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from pathlib import Path
import threading
from typing import Dict, List, Tuple, Any, Optional


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
class ImporterGUI:
    """
    Graphical user interface for the JSON to MySQL importer.
    
    This class handles all UI interactions and delegates business logic
    to the JSONtoMySQL class. It follows the Model-View pattern where
    JSONtoMySQL is the Model and this class is the View.
    """
    
    # Configuration file for saving connection settings
    CONFIG_FILE = "importer_config.json"
    
    def __init__(self, root):
        """Initialize the GUI components."""
        self.root = root
        self.root.title("JSON to MySQL Importer")
        self.root.geometry("650x700")
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
        
        # Initial state - disable import button
        self.update_import_button_state()
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
        """Create directory selection."""
        frame = tk.LabelFrame(self.root, text="JSON Files Location", padx=10, pady=10)
        frame.pack(padx=10, pady=10, fill="x")
        
        self.directory_var = tk.StringVar()
        self.directory_var.trace('w', lambda *args: self.update_import_button_state())
        
        tk.Entry(frame, textvariable=self.directory_var, width=50, state="readonly").pack(side="left", padx=5)
        tk.Button(frame, text="Browse...", command=self.browse_directory, width=10).pack(side="left")
    def create_progress_bar(self):
        """Create progress bar for import operations."""
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
        """Create execute buttons."""
        # Import button
        self.execute_btn = tk.Button(
            self.root,
            text="Import JSON Exception Files from EJ",
            command=self.execute_import,
            bg="#4CAF50",
            fg="white",
            font=("Arial", 12, "bold"),
            height=2,
            state="disabled"  # Initially disabled
        )
        self.execute_btn.pack(padx=10, pady=(10,5), fill="x")

        # Export button (placeholder for future functionality)
        self.export_btn = tk.Button(
            self.root,
            text="Future Enhancement: Export PostScript_AllianceMerge to JSON Files",
            command=lambda: None,  # Placeholder - no functionality yet
            bg="#2196F3",
            fg="white",
            font=("Arial", 12, "bold"),
            height=2,
            state="disabled"  # Disabled until we add this functionality
        )
        self.export_btn.pack(padx=10, pady=(0,10), fill="x")
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
        self.update_import_button_state()
    def update_import_button_state(self):
        """
        Enable/disable import button based on prerequisites.
        
        Import button is only enabled when:
        1. Connection has been tested successfully
        2. A directory has been selected
        """
        if self.connection_verified and self.directory_var.get().strip():
            self.execute_btn.config(state="normal")
        else:
            self.execute_btn.config(state="disabled")
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
            self.update_import_button_state()
    def browse_directory(self):
        """Open directory browser dialog."""
        directory = filedialog.askdirectory(title="Select JSON Files Directory")
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
            messagebox.showerror("Validation Error", "JSON files directory is required")
            return False
        
        return True
    def execute_import(self):
        """Execute the import process."""
        if not self.validate_import_inputs():
            return
        
        # Disable buttons during import
        self.execute_btn.config(state="disabled")
        self.test_conn_btn.config(state="disabled")
        
        # Clear status window and reset progress bar
        self.status_text.config(state="normal")
        self.status_text.delete(1.0, "end")
        self.status_text.config(state="disabled")
        self.progress_bar["value"] = 0
        
        # Run import in separate thread to prevent UI freezing
        thread = threading.Thread(target=self.run_import)
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