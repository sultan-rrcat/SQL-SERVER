import os
import pyodbc
import logging
from datetime import datetime
import time
import threading
import json
import traceback
from pathlib import Path

# ================= CONFIGURATION =================

config = {
    'server': 'DESKTOP-4VRJ480',
    'database': 'INDUS1_RSS',
    'source_backup_folder': 'D:\\BACKUP\\INDUS1_RSS',
    'target_backup_folder': 'E:\\BACKUP',
    'restore_path': 'E:\\SQLData',
    'use_windows_auth': True,
    'username': 'sa',
    'password': 'password',
    'source_master_file': 'E:\\BACKUP\\INDUS1_RF_2024_2025_Partitioned_MASTER_20251229_194406.bak'
}

PARTITION_YEARS = [2022, 2023, 2024, 2025, 2026]
REQUIRED_BACKUP_YEAR = [2025]

# ================= LOGGING SETUP =================
log_file = r'C:\Users\ADMIN\Desktop\PARTITIONING\AUTOMATION\db_automation.log'
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

def _get_connection_string():
    """Generate connection string based on configuration."""
    try:
        if config['use_windows_auth']:
            return f"DRIVER={{SQL Server}};SERVER={config['server']};Trusted_Connection=yes;"
        else:
            return f"DRIVER={{SQL Server}};SERVER={config['server']};UID={config['username']};PWD={config['password']}"
    except KeyError as e:
        logger.error(f"Missing configuration key: {e}")
        raise

def scan_backup_folder(source_backup_folder):
    """Scan folder for backup files and read their headers."""
    logger.info(f"Scanning backup folder: {source_backup_folder}")
    
    if not os.path.exists(source_backup_folder):
        error_msg = f"Backup folder not found: {source_backup_folder}"
        logger.error(error_msg)
        return False, error_msg
    
    backup_files = []
    try:
        for ext in ['.bak', '.trn']:
            backup_files.extend(Path(source_backup_folder).glob(f'*{ext}'))
        
        logger.info(f"Found {len(backup_files)} backup file(s) in folder")
    except Exception as e:
        error_msg = f"Error scanning folder: {str(e)}"
        logger.error(error_msg)
        return False, error_msg
    
    if not backup_files:
        error_msg = "No backup files found in folder"
        logger.warning(error_msg)
        return False, error_msg

    backups_info = []
    conn = None
    cursor = None
    
    try:
        conn = pyodbc.connect(_get_connection_string(), autocommit=True)
        cursor = conn.cursor()

        for backup_path in backup_files:
            try:
                logger.debug(f"Reading header from: {backup_path}")
                cursor.execute(f"RESTORE HEADERONLY FROM DISK = '{str(backup_path)}'")
                
                if cursor.description is None:
                    logger.warning(f"Could not read header from {backup_path}")
                    continue

                columns = [column[0] for column in cursor.description]
                backup_info = cursor.fetchone()

                if backup_info:
                    info_dict = dict(zip(columns, backup_info))
                    info_dict['FilePath'] = str(backup_path)
                    backups_info.append(info_dict)
                    backup_type = info_dict.get('BackupTypeDescription', 'Unknown')
                    logger.info(f"✓ Valid backup: {backup_path.name} (Type: {backup_type})")
                else:
                    logger.warning(f"Empty header info from {backup_path}")
            
            except pyodbc.Error as e:
                logger.warning(f"Error reading {backup_path.name}: {str(e)}")
                continue
            except Exception as e:
                logger.warning(f"Unexpected error reading {backup_path.name}: {str(e)}")
                continue
        
        if not backups_info:
            error_msg = "No valid backup files could be read"
            logger.error(error_msg)
            return False, error_msg
        
        full_backups = [b for b in backups_info if b.get('BackupType') == 1]
        log_backups = [b for b in backups_info if b.get('BackupType') == 2]

        logger.info(f"Found {len(full_backups)} full backup(s) and {len(log_backups)} log backup(s)")

        if not full_backups:
            error_msg = "No full backup found in folder"
            logger.error(error_msg)
            return False, error_msg
        
        full_backups.sort(key=lambda x: x.get('CheckpointLSN', 0), reverse=True)
        log_backups.sort(key=lambda x: x.get('FirstLSN', 0))

        sorted_backups = [full_backups[0]] + log_backups
        logger.info(f"Selected backup chain: {len(sorted_backups)} file(s)")
        return True, sorted_backups

    except pyodbc.Error as e:
        error_msg = f"Database error reading backup headers: {str(e)}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error scanning backups: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, error_msg
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def check_backup_file(backup_path):
    """Validate backup file exists and is accessible."""
    logger.debug(f"Validating backup file: {backup_path}")
    
    if not os.path.exists(backup_path):
        error_msg = f"Backup file not found: {backup_path}"
        logger.error(error_msg)
        return False, error_msg
    
    if not os.access(backup_path, os.R_OK):
        error_msg = f"No read permission for backup file: {backup_path}"
        logger.error(error_msg)
        return False, error_msg

    if not str(backup_path).lower().endswith(('.bak', '.trn')):
        error_msg = "Backup file must have .bak or .trn extension"
        logger.error(error_msg)
        return False, error_msg
    
    logger.debug("Backup file validation passed")
    return True, "Backup file is valid"

def get_database_info(database_name):
    """Retrieve database information from SQL Server."""
    logger.debug(f"Checking database info for: {database_name}")
    conn = None
    cursor = None
    
    try:
        conn = pyodbc.connect(_get_connection_string(), autocommit=True)
        cursor = conn.cursor()

        cursor.execute(f"""
        SELECT name, state_desc, recovery_model_desc, physical_database_name
        FROM sys.databases
        WHERE name='{database_name}'
        """)

        result = cursor.fetchone()
        if result:
            logger.info(f"Database exists - Name: {result[0]}, State: {result[1]}, Recovery: {result[2]}")
        else:
            logger.info(f"Database '{database_name}' does not exist")
        return result
        
    except pyodbc.Error as e:
        logger.error(f"Error checking database info: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error checking database: {str(e)}")
        return None
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def restore_database_from_folder(database, source_backup_folder, restore_path=None, overwrite=True, stats_interval=10, callback=None):
    """Restore database from a folder containing backup files."""
    logger.info("="*80)
    logger.info("Starting database restore operation")
    logger.info("="*80)
    
    restore_complete = [False]
    monitor_thread = None
    
    def progress_monitor():
        """Monitor restore progress in a separate thread."""
        monitor_conn = None
        monitor_cursor = None
        try:
            monitor_conn = pyodbc.connect(_get_connection_string())
            monitor_cursor = monitor_conn.cursor()

            last_percent = 0

            while not restore_complete[0]:
                try:
                    monitor_cursor.execute(f"""
                    SELECT 
                        r.percent_complete,
                        r.estimated_completion_time,
                        r.command,
                        r.start_time
                    FROM sys.dm_exec_requests r
                    WHERE r.command LIKE 'RESTORE%'
                    AND r.database_id = DB_ID('{database}')
                    """)
                    
                    progress = monitor_cursor.fetchone()

                    if progress:
                        percent_complete = progress[0]

                        if percent_complete >= last_percent + stats_interval:
                            if callback:
                                callback({
                                    'percent_complete': percent_complete,
                                    'estimated_completion': progress[1],
                                    'status': 'Restoring...'
                                })
                            logger.info(f"Restore progress: {percent_complete:.1f}%")
                            last_percent = percent_complete
                except Exception as e:
                    logger.debug(f"Progress monitor query error: {str(e)}")
                    
                time.sleep(2)

        except Exception as e:
            logger.error(f"Progress monitor error: {str(e)}")
            if callback:
                callback({'error': str(e), 'status':'Progress monitor error'})
        finally:
            try:
                if monitor_cursor:
                    monitor_cursor.close()
                if monitor_conn:
                    monitor_conn.close()
            except:
                pass
    
    conn = None
    cursor = None
    
    try:
        success, backups_info = scan_backup_folder(source_backup_folder)
        if not success:
            return False, backups_info
        
        if callback:
            callback({
                'status': f'Found {len(backups_info)} backup(s) in folder',
                'backups': backups_info
            })
        
        logger.info(f"Connecting to SQL Server: {config['server']}")
        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()
        logger.info("Connection established successfully")

        for idx, backup_info in enumerate(backups_info):
            backup_path = backup_info['FilePath']
            backup_type_desc = backup_info.get('BackupTypeDescription', 'Unknown')
            backup_type = backup_info.get('BackupType', 0)

            logger.info("-" * 60)
            logger.info(f"Processing backup {idx+1}/{len(backups_info)}")
            logger.info(f"Type: {backup_type_desc}")
            logger.info(f"File: {Path(backup_path).name}")
            
            if callback:
                callback({
                    'status': f'Restoring {backup_type_desc} ({idx+1}/{len(backups_info)})',
                    'backup_info': backup_info
                })
            
            restore_command = None

            if backup_type == 1 or 'Database' in str(backup_type_desc):
                logger.info("Building RESTORE DATABASE command...")
                cmd_parts = [f"RESTORE DATABASE [{database}] FROM DISK='{backup_path}'"]
                
                try:
                    cursor.execute(f"RESTORE FILELISTONLY FROM DISK='{backup_path}'")
                    files = cursor.fetchall()
                    logger.info(f"Backup contains {len(files)} file(s)")

                    move_clauses = []
                    if restore_path:
                        logger.info(f"Using custom restore path: {restore_path}")
                        for file_info in files:
                            logical_name = file_info[0]
                            file_type = file_info[2]
                            ext = '.mdf' if file_type == 'D' else '.ldf'
                            new_path = os.path.join(restore_path, f"{database}_{ext}")
                            move_clauses.append(f"MOVE '{logical_name}' TO '{new_path}'")
                            logger.debug(f"Will move {logical_name} to {new_path}")
                    
                    if move_clauses:
                        cmd_parts.append("WITH")
                        cmd_parts.append(", ".join(move_clauses))
                        if overwrite and idx == 0:
                            cmd_parts.append(", REPLACE")
                            logger.info("Using REPLACE option to overwrite existing database")
                        cmd_parts.append(f", STATS={stats_interval}")
                        if len(backups_info) > 1:
                             cmd_parts.append(", NORECOVERY")
                             logger.info("Using NORECOVERY (more backups to apply)")
                    else:
                        with_opts = []
                        if overwrite and idx == 0: 
                            with_opts.append("REPLACE")
                            logger.info("Using REPLACE option to overwrite existing database")
                        if len(backups_info) > 1: 
                            with_opts.append("NORECOVERY")
                            logger.info("Using NORECOVERY (more backups to apply)")
                        with_opts.append(f"STATS={stats_interval}")
                        cmd_parts.append("WITH " + ", ".join(with_opts))
                    
                    restore_command = " ".join(cmd_parts)
                    
                except Exception as e:
                    logger.error(f"Error reading file list: {str(e)}")
                    raise
            
            elif backup_type == 2 or 'Transaction Log' in str(backup_type_desc):
                logger.info("Building RESTORE LOG command...")
                restore_command = f"RESTORE LOG [{database}] FROM DISK='{backup_path}'"
                opts = []
                if idx < len(backups_info) - 1:
                    opts.append("NORECOVERY")
                    logger.info("Using NORECOVERY (more logs to apply)")
                else:
                    opts.append("RECOVERY")
                    logger.info("Using RECOVERY (final restore)")
                
                opts.append(f"STATS={stats_interval}")
                restore_command += " WITH " + ", ".join(opts)
            
            else:
                logger.warning(f"Skipping unsupported backup type: {backup_type_desc}")
                continue
            
            if not restore_command:
                error_msg = "Failed to generate restore command"
                logger.error(error_msg)
                raise Exception(error_msg)

            logger.info("Executing restore command...")
            logger.debug(f"Command: {restore_command}")
            
            if callback:
                callback({'status': 'Executing restore command...', 'command': restore_command})
            
            if backup_type == 1:
                restore_complete[0] = False
                monitor_thread = threading.Thread(target=progress_monitor)
                monitor_thread.daemon = True
                monitor_thread.start()
                logger.info("Progress monitor started")

            start_time = time.time()
            
            try:
                cursor.execute(restore_command)
                while cursor.nextset():
                    pass
            except pyodbc.Error as e:
                logger.error(f"SQL Server error during restore: {str(e)}")
                raise
            
            if backup_type == 1:
                restore_complete[0] = True

            elapsed_time = time.time() - start_time
            logger.info(f"✓ {backup_type_desc} restore completed in {elapsed_time:.2f} seconds")

            if callback:
                callback({
                    'status': f'{backup_type_desc} restore completed',
                    'elapsed_time': elapsed_time
                })
                
        logger.info("-" * 60)
        logger.info("Checking final database state...")
        cursor.execute(f"SELECT state_desc FROM sys.databases WHERE name='{database}'")
        db_state = cursor.fetchone()

        if db_state:
            logger.info(f"✓ Database state: {db_state[0]}")
        else:
            logger.warning("Could not retrieve database state")

        if callback:
            callback({
                'status': 'COMPLETED',
                'database_state': db_state[0] if db_state else 'Unknown'
            })

        logger.info("="*80)
        logger.info("✓ RESTORE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        return True, "Restore completed successfully."

    except pyodbc.Error as e:
        restore_complete[0] = True
        error_msg = f"SQL Server error: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, error_msg
    except Exception as e:
        restore_complete[0] = True
        error_msg = f"Error: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, error_msg
    finally:
        restore_complete[0] = True
        if monitor_thread and monitor_thread.is_alive():
            logger.debug("Waiting for monitor thread to complete...")
            monitor_thread.join(timeout=5)
        
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            logger.debug("Database connections closed")
        except Exception as e:
            logger.warning(f"Error closing connections: {str(e)}")

def _escape_sql_literal(s: str) -> str:
    """Escape single quotes for SQL string literals and ensure it's a str."""
    if s is None:
        return ''
    return str(s).replace("'", "''")

def restore_database_from_file(database,
                               source_master_file,
                               restore_path=None,
                               overwrite=True,
                               stats_interval=10,
                               callback=None,
                               trusted_connection=True,
                               uid=None,
                               pwd=None):
    """Restore a SQL Server database from a backup file."""
    conn = None
    cursor = None
    db_exists = False

    try:
        logger.info("Connecting to SQL Server")
        conn = pyodbc.connect(_get_connection_string(),
                              autocommit=True, timeout=0)
        cursor = conn.cursor()

        # 1) Get logical file names from backup
        source_master_file = os.path.normpath(source_master_file)
        sql_filelist = f"RESTORE FILELISTONLY FROM DISK = N'{_escape_sql_literal(source_master_file)}';"
        logger.debug("Executing FILELISTONLY: %s", sql_filelist)
        cursor.execute(sql_filelist)
        rows = cursor.fetchall()
        if not rows:
            error_msg = "Could not read file list from backup."
            logger.error(error_msg)
            return False, error_msg

        desc = [d[0].lower() for d in cursor.description]
        def col(row, name):
            try:
                return row[desc.index(name.lower())]
            except ValueError:
                return None

        file_moves = []
        for r in rows:
            logical_name = col(r, 'LogicalName') or r[0]
            physical_name = col(r, 'PhysicalName') or r[1]
            filename = os.path.basename(physical_name)

            if restore_path:
                # Ensure unique filenames for data vs log
                if "log" in logical_name.lower():
                    target_path = os.path.join(restore_path, f"{database}_log.ldf")
                else:
                    target_path = os.path.join(restore_path, f"{database}.mdf")
            else:
                target_path = physical_name

            file_moves.append((logical_name, target_path))

        if not file_moves:
            error_msg = "No files found in backup file list."
            logger.error(error_msg)
            return False, error_msg

        # 2) If overwrite requested, set SINGLE_USER
        cursor.execute("SELECT 1 FROM sys.databases WHERE name = ?", database)
        if cursor.fetchone():
            db_exists = True

        if overwrite and db_exists:
            try:
                logger.info("Setting database to SINGLE_USER")
                cursor.execute(
                    f"ALTER DATABASE [{database.replace(']', ']]')}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
                )
            except Exception as e:
                logger.warning("Could not set SINGLE_USER: %s", e)

        # 3) Build WITH options
        with_opts = [f"MOVE N'{_escape_sql_literal(logical)}' TO N'{_escape_sql_literal(target)}'"
                     for logical, target in file_moves]
        if overwrite:
            with_opts.append("REPLACE")
        if stats_interval and int(stats_interval) > 0:
            with_opts.append(f"STATS = {int(stats_interval)}")
        
        # Add RECOVERY to ensure database comes online
        with_opts.append("RECOVERY")

        with_clause = ", ".join(with_opts)
        restore_sql = (
            f"RESTORE DATABASE [{database.replace(']', ']]')}] "
            f"FROM DISK = N'{_escape_sql_literal(source_master_file)}' "
            f"WITH {with_clause};"
        )

        logger.info("Executing RESTORE statement")
        logger.debug("\n%s", restore_sql)
        
        if callback:
            callback({'status': 'Starting restore...', 'percent_complete': 0})
        
        cursor.execute(restore_sql)

        # Wait for restore to complete
        logger.info("Waiting for restore to complete...")
        while cursor.nextset():
            pass

        # 4) Monitor progress
        logger.info("Monitoring restore progress...")
        max_wait = 3600  # 60 minutes max
        waited = 0
        last_percent = 0
        
        while waited < max_wait:
            time.sleep(2)
            waited += 2
            
            try:
                # Check if restore is still running
                cursor.execute(
                    "SELECT percent_complete, status "
                    "FROM sys.dm_exec_requests "
                    "WHERE command LIKE 'RESTORE%' AND database_id = DB_ID(?)",
                    database
                )
                row = cursor.fetchone()
                
                if row:
                    percent = int(row[0] or 0)
                    status = row[1] or ''
                    
                    # Only log if progress changed significantly
                    if percent >= last_percent + 5:
                        logger.info(f"Restore progress: {percent}%")
                        if callback:
                            callback({'percent_complete': percent, 'status': status})
                        last_percent = percent
                    
                    if percent >= 100:
                        break
                else:
                    # No active restore command, check database state
                    cursor.execute("SELECT state_desc FROM sys.databases WHERE name = ?", database)
                    s = cursor.fetchone()
                    if s:
                        if s[0] == 'ONLINE':
                            logger.info("Restore completed - database is ONLINE")
                            if callback:
                                callback({'status': 'Restore complete', 'percent_complete': 100})
                            break
                        elif s[0] == 'RESTORING':
                            logger.info("Database still in RESTORING state, waiting...")
                        else:
                            logger.warning(f"Database in unexpected state: {s[0]}")
                            break
                    else:
                        logger.warning("Database not found in sys.databases yet")
                        
            except Exception as e:
                logger.debug(f"Progress polling error (may be normal): {e}")

        # Verify database is restored and ONLINE
        logger.info("Verifying final database state...")
        max_retries = 30
        for i in range(max_retries):
            try:
                cursor.execute("SELECT state_desc FROM sys.databases WHERE name = ?", database)
                db_state = cursor.fetchone()
                
                if db_state:
                    state = db_state[0]
                    
                    if state == 'ONLINE':
                        logger.info(f"✓ Database restored successfully and is ONLINE")
                        if callback:
                            callback({'status': 'Database ONLINE', 'percent_complete': 100})
                        return True, f"Database restored successfully. State: {state}"
                    elif state == 'RESTORING':
                        # If still restoring after command completed, try explicit RECOVERY
                        if i == 0:
                            try:
                                logger.info("Database still in RESTORING state, attempting explicit RECOVERY...")
                                cursor.execute(f"RESTORE DATABASE [{database.replace(']', ']]')}] WITH RECOVERY;")
                                while cursor.nextset():
                                    pass
                                logger.info("RECOVERY command executed, waiting for database to come online...")
                            except Exception as e:
                                logger.warning(f"Could not execute RESTORE WITH RECOVERY: {e}")
                        
                        if i < max_retries - 1:
                            logger.info(f"Waiting for database to come online... ({i+1}/{max_retries})")
                            time.sleep(2)
                        else:
                            error_msg = "Database stuck in RESTORING state"
                            logger.error(error_msg)
                            return False, error_msg
                    else:
                        error_msg = f"Database in unexpected state: {state}"
                        logger.error(error_msg)
                        return False, error_msg
                else:
                    if i < max_retries - 1:
                        logger.warning(f"Database not found, retry {i+1}/{max_retries}")
                        time.sleep(2)
                    else:
                        error_msg = "Database not found after restore"
                        logger.error(error_msg)
                        return False, error_msg
            except Exception as e:
                logger.warning(f"Error checking database state: {e}")
                time.sleep(2)
        
        # If we get here, something went wrong
        error_msg = "Failed to verify database state within timeout"
        logger.error(error_msg)
        return False, error_msg

    except Exception as exc:
        error_msg = f"Unexpected error in restoration: {exc}"
        logger.exception(error_msg)
        return False, error_msg
    finally:
        try:
            if overwrite and db_exists and cursor:
                # Only try to set MULTI_USER if database is accessible
                try:
                    cursor.execute("SELECT state_desc FROM sys.databases WHERE name = ?", database)
                    db_state = cursor.fetchone()
                    if db_state and db_state[0] == 'ONLINE':
                        logger.info("Restoring database to MULTI_USER")
                        cursor.execute(f"ALTER DATABASE [{database.replace(']', ']]')}] SET MULTI_USER;")
                except Exception as e:
                    logger.warning("Could not set MULTI_USER: %s", e)
        except Exception as e:
            logger.warning("Error in finally block: %s", e)
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def kill_database_connections(database_name):
    """Kill all active connections to a database."""
    logger.info(f"Killing all connections to database: {database_name}")
    conn = None
    cursor = None
    
    try:
        conn = pyodbc.connect(_get_connection_string(), autocommit=True)
        cursor = conn.cursor()
        
        logger.info("Setting database to SINGLE_USER mode...")
        cursor.execute(f"""
        ALTER DATABASE [{database_name}]
        SET SINGLE_USER
        WITH ROLLBACK IMMEDIATE
        """)
        
        logger.info("Restoring MULTI_USER mode...")
        cursor.execute(f"ALTER DATABASE [{database_name}] SET MULTI_USER")
        
        logger.info("✓ All connections killed successfully")
        return True
        
    except pyodbc.Error as e:
        logger.error(f"Error killing database connections: {str(e)}")
        logger.debug(traceback.format_exc())
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.debug(traceback.format_exc())
        return False
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def progress_callback(info):
    """Callback function for restore progress updates."""
    # Handle both dict-style and tuple-style callbacks
    if isinstance(info, dict):
        if 'percent_complete' in info:
            logger.info(f"Progress: {info['percent_complete']:.1f}% complete")
        elif 'status' in info:
            logger.info(f"Status: {info['status']}")
        if 'error' in info:
            logger.error(f"Error: {info['error']}")
    else:
        # Handle the case where it's called with separate arguments (shouldn't happen now)
        logger.info(f"Progress update: {info}")

def restoration(type): 
    """Main restoration function."""
    if type == 'SCRATCH':
        try:
            logger.info("Checking if database exists...")
            db_info = get_database_info(config['database'])
            
            if db_info:
                logger.info(f"Database '{config['database']}' exists")
                logger.info(f"Current state: {db_info[1]}")
                logger.info(f"Recovery model: {db_info[2]}")
                
                confirm = input("\n⚠ Kill all connections and restore? (y/n): ")
                if confirm.lower() == 'y':
                    logger.info("User confirmed restoration")
                    if not kill_database_connections(config['database']):
                        logger.error("Failed to kill database connections")
                        return False
                else:
                    logger.info("User cancelled restoration")
                    return False
            else:
                logger.info(f"Database '{config['database']}' does not exist - will create new")

            logger.info(f"Starting restore of '{config['database']}'")
            logger.info(f"Source folder: {config['source_backup_folder']}")
            logger.info(f"Restore path: {config['restore_path']}")
            
            success, message = restore_database_from_folder(
                database=config['database'],
                source_backup_folder=config['source_backup_folder'],
                restore_path=config['restore_path'],
                overwrite=True,
                stats_interval=5,
                callback=progress_callback
            )

            if success:
                logger.info(f"✅ {message}")
                return True
            else:
                logger.error(f"❌ {message}")
                return False
                
        except KeyboardInterrupt:
            logger.warning("Restoration cancelled by user (Ctrl+C)")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in restoration: {str(e)}")
            logger.debug(traceback.format_exc())
            return False

    elif type=='MASTER':
        try:
            logger.info("Checking if database exists...")
            db_info = get_database_info(config['database'])
            
            if db_info:
                logger.info(f"Database '{config['database']}' exists")
                logger.info(f"Current state: {db_info[1]}")
                logger.info(f"Recovery model: {db_info[2]}")
                logger.info(f"Source file: {config['source_master_file']}")
                
                confirm = input("\n⚠ Kill all connections and restore? (y/n): ")
                if confirm.lower() == 'y':
                    logger.info("User confirmed restoration")
                    if not kill_database_connections(config['database']):
                        logger.error("Failed to kill database connections")
                        return False
                else:
                    logger.info("User cancelled restoration")
                    return False
            else:
                logger.info(f"Database '{config['database']}' does not exist - will create new")

            logger.info(f"Starting restore of '{config['database']}'")
            logger.info(f"Restore path: {config['restore_path']}")
            
            success, message = restore_database_from_file(
                database=config['database'],
                source_master_file=config['source_master_file'],
                restore_path=config['restore_path'],
                overwrite=True,
                stats_interval=5,
                callback=progress_callback
            )

            if success:
                logger.info(f"✅ {message}")
                return True
            else:
                logger.error(f"❌ {message}")
                return False
                
        except KeyboardInterrupt:
            logger.warning("Restoration cancelled by user (Ctrl+C)")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in restoration: {str(e)}")
            logger.debug(traceback.format_exc())
            return False
    else:
        return False

def prepare_database():
    """Prepare database for partitioning."""
    logger.info("="*80)
    logger.info("PREPARING DATABASE")
    logger.info("="*80)
    
    conn = None
    cursor = None
    
    try:
        logger.info("Connecting to database...")
        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()

        # 1. Set Recovery Model
        logger.info(f"Setting recovery model for [{config['database']}] to SIMPLE...")
        cursor.execute(f"ALTER DATABASE [{config['database']}] SET RECOVERY SIMPLE")
        logger.info("✓ Recovery model set to SIMPLE")
        
        # 2. Identify indexes to drop
        logger.info("Identifying target clustered indexes...")

        # Switch to the target database
        cursor.execute(f"USE [{config['database']}]")

        # Now run the SELECT query
        find_idx_sql = """
        SELECT  
            'DROP INDEX [' + i.name + '] ON [' + SCHEMA_NAME(t.schema_id) + '].[' + t.name + ']' AS drop_command,
            t.name AS table_name,
            i.name AS index_name
        FROM sys.tables t
        JOIN sys.indexes i ON i.object_id = t.object_id
        WHERE i.type_desc = 'CLUSTERED'
        AND i.is_primary_key = 0
        AND (
            t.name LIKE 'main_%' 
            OR t.name IN (SELECT REPLACE(name, 'main_', '') FROM sys.tables WHERE name LIKE 'main_%')
        );
        """

        cursor.execute(find_idx_sql)
        rows = cursor.fetchall()

        if not rows:
            logger.info("No matching clustered indexes found to drop.")
        else:
            logger.info(f"Found {len(rows)} clustered index(es) to drop.")
            
            # 3. Loop through and execute each drop command
            for row in rows:
                drop_cmd = row.drop_command
                table_name = row.table_name
                
                # logger.info(f"Dropping index on table [{table_name}]...")
                logger.debug(f"Executing: {drop_cmd}") # Detailed log for debugging
                
                cursor.execute(drop_cmd)
                
                logger.info(f"✓ Successfully dropped index from {table_name}")

        logger.info("✓ Database preparation completed")
        
    except pyodbc.Error as e:
        logger.error(f"SQL Server error during database preparation: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"Unexpected error during database preparation: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def create_partitions():
    """Create partition function and scheme."""
    logger.info("="*80)
    logger.info("CREATING PARTITIONS")
    logger.info("="*80)
    
    conn = None
    cursor = None
    
    try:
        logger.info(f"Partition years: {PARTITION_YEARS}")
        pf_values = ",".join([f"'{year}-01-01 00:00:00'" for year in PARTITION_YEARS])
        logger.debug(f"Partition function values: {pf_values}")
        
        conn = pyodbc.connect(_get_connection_string(), autocommit=True)
        cursor = conn.cursor()
        
        logger.info("Checking for existing partition scheme...")
        setup_partition_sql = f"""
        USE [{config['database']}];
        
        IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name='PS_LOGTIME_YEARLY')
        BEGIN
            PRINT 'Dropping existing partition scheme: PS_LOGTIME_YEARLY';
            DROP PARTITION SCHEME PS_LOGTIME_YEARLY;
        END
            
        IF EXISTS (SELECT * FROM sys.partition_functions WHERE name='PF_LOGTIME_YEARLY')
        BEGIN
            PRINT 'Dropping existing partition function: PF_LOGTIME_YEARLY';
            DROP PARTITION FUNCTION PF_LOGTIME_YEARLY;
        END
        
        PRINT 'Creating partition function: PF_LOGTIME_YEARLY';
        CREATE PARTITION FUNCTION PF_LOGTIME_YEARLY (DATETIME)
        AS RANGE RIGHT FOR VALUES ({pf_values});

        PRINT 'Creating partition scheme: PS_LOGTIME_YEARLY';
        CREATE PARTITION SCHEME PS_LOGTIME_YEARLY
        AS PARTITION PF_LOGTIME_YEARLY ALL TO ([PRIMARY]);
        
        PRINT 'Partition function and scheme created successfully';
        """

        cursor.execute(setup_partition_sql)
        
        while cursor.nextset():
            pass
            
        logger.info("✓ Partition function and scheme created successfully")
        
    except pyodbc.Error as e:
        logger.error(f"SQL Server error creating partitions: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating partitions: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def create_indexes():
    """Create clustered indexes on partition scheme."""
    logger.info("="*80)
    logger.info("CREATING INDEXES")
    logger.info("="*80)
    
    conn = None
    cursor = None
    
    try:
        logger.info("Connecting to database...")
        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()

        logger.info("Building index creation script...")
        create_idx_sql = f"""
        USE [{config['database']}];
        DECLARE @sql NVARCHAR(MAX) = N'';
        DECLARE @count INT = 0;
        
        SELECT @sql = STRING_AGG(
            CAST('CREATE CLUSTERED INDEX [CIX_' + t.name + '] ON [dbo].[' + t.name + '](logtime) ON PS_LOGTIME_YEARLY(logtime);' AS NVARCHAR(MAX)),
            CHAR(10)
        ),
        @count = COUNT(*)
        FROM sys.tables t
        WHERE (t.name LIKE 'main_%'
        OR t.name IN (SELECT REPLACE(name, 'main_', '') FROM sys.tables WHERE name LIKE 'main_%'))
        AND NOT EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.type_desc = 'CLUSTERED');

        IF @sql IS NOT NULL
        BEGIN
            PRINT 'Creating ' + CAST(@count AS VARCHAR(10)) + ' clustered index(es)...';
            EXEC sp_executesql @sql;
            PRINT 'Indexes created successfully';
        END
        ELSE
        BEGIN
            PRINT 'No indexes to create';
        END
        """
        
        logger.info("Executing index creation...")
        cursor.execute(create_idx_sql)
        
        while cursor.nextset():
            pass
            
        logger.info("✓ Indexes created successfully")
        
    except pyodbc.Error as e:
        logger.error(f"SQL Server error creating indexes: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating indexes: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def create_temp_tables():
    """Create temporary staging tables for partition switching."""
    logger.info("="*80)
    logger.info("CREATING TEMPORARY TABLES")
    logger.info("="*80)
    
    conn = None
    cursor = None
    
    try:
        logger.info("Connecting to database...")
        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()

        logger.info("Creating temp tables with partition scheme...")
        create_temp_sql = f"""
        USE [{config['database']}];
        DECLARE @tbl NVARCHAR(255);
        DECLARE @cursor CURSOR;
        DECLARE @count INT = 0;

        SET @cursor = CURSOR FOR 
        SELECT name FROM sys.tables
        WHERE (name LIKE 'main_%' OR name IN (SELECT REPLACE(name, 'main_', '') 
        FROM sys.tables WHERE name LIKE 'main_%'))
        AND name NOT LIKE 'temp_%';

        OPEN @cursor;
        FETCH NEXT FROM @cursor INTO @tbl;

        WHILE @@FETCH_STATUS = 0
        BEGIN 
            SET @count = @count + 1;
            
            IF OBJECT_ID('dbo.temp_' + @tbl) IS NOT NULL
            BEGIN
                PRINT 'Dropping existing temp table: temp_' + @tbl;
                EXEC('DROP TABLE [dbo].[temp_' + @tbl + ']');
            END

            PRINT 'Creating temp table: temp_' + @tbl;
            EXEC('SELECT * INTO [dbo].[temp_' + @tbl + '] FROM [dbo].[' + @tbl + '] WHERE 1=0');
            
            PRINT 'Creating clustered index on temp_' + @tbl;
            EXEC('CREATE CLUSTERED INDEX [CIX_temp_' + @tbl + '] ON [dbo].[temp_' + @tbl + '](logtime) ON PS_LOGTIME_YEARLY(logtime)');

            FETCH NEXT FROM @cursor INTO @tbl;
        END 
        
        CLOSE @cursor;
        DEALLOCATE @cursor;
        
        PRINT 'Created ' + CAST(@count AS VARCHAR(10)) + ' temp table(s)';
        """
        
        cursor.execute(create_temp_sql)
        
        while cursor.nextset():
            pass
            
        logger.info("✓ Temp tables created successfully")
        
    except pyodbc.Error as e:
        logger.error(f"SQL Server error creating temp tables: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating temp tables: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def create_master_backup():
    """Create full backup of partitioned database."""
    logger.info("="*80)
    logger.info("CREATING MASTER BACKUP")
    logger.info("="*80)
    
    conn = None
    cursor = None
    
    try:
        logger.info(f"Target backup folder: {config['target_backup_folder']}")
        os.makedirs(config['target_backup_folder'], exist_ok=True)
        logger.info("✓ Backup folder ready")

        master_bkp_path = os.path.join(
            config['target_backup_folder'], 
            f"{config['database']}_Partitioned_MASTER_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
        )
        
        logger.info(f"Backup file: {master_bkp_path}")
        logger.info("Starting backup (this may take a while)...")
        
        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()
        
        bkp_sql = f"BACKUP DATABASE [{config['database']}] TO DISK='{master_bkp_path}' WITH FORMAT, COMPRESSION, STATS=10"
        
        cursor.execute(bkp_sql)
        
        while cursor.nextset():
            pass
        
        if os.path.exists(master_bkp_path):
            file_size = os.path.getsize(master_bkp_path) / (1024 * 1024 * 1024)  # GB
            logger.info(f"✓ Backup created successfully ({file_size:.2f} GB)")
            config['source_master_file']=master_bkp_path
        else:
            logger.warning("Backup command completed but file not found")
            
    except pyodbc.Error as e:
        logger.error(f"SQL Server error during backup: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"Unexpected error during backup: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def purge_years():
    """Purge data for specified years using partition switching."""
    logger.info("="*80)
    logger.info("PURGING YEARS")
    logger.info("="*80)
    
    conn = None
    cursor = None
    
    try:
        YEARS_TO_PURGE = list(set(PARTITION_YEARS) - set(REQUIRED_BACKUP_YEAR))
        logger.info(f"Years to purge: {YEARS_TO_PURGE}")
        logger.info(f"Years to keep: {REQUIRED_BACKUP_YEAR}")
        
        if not YEARS_TO_PURGE:
            logger.info("No years to purge")
            return
        
        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()

        for year in YEARS_TO_PURGE:
            logger.info("-" * 60)
            logger.info(f"Purging year: {year}")
            
            purge_sql = f"""
            USE [{config['database']}];
            DECLARE @PartitionID INT;
            SELECT @PartitionID = $PARTITION.PF_LOGTIME_YEARLY('{year}-01-01');
            
            PRINT 'Partition ID for year {year}: ' + CAST(@PartitionID AS VARCHAR(10));
            
            DECLARE @sql NVARCHAR(MAX) = N'';
            DECLARE @tbl NVARCHAR(255);
            DECLARE @rowcount BIGINT;
            DECLARE @totalRows BIGINT = 0;
            DECLARE @tableCount INT = 0;

            DECLARE @cursor CURSOR;
            SET @cursor = CURSOR FOR
            SELECT name FROM sys.tables
            WHERE (name LIKE 'main_%' OR name IN (SELECT REPLACE(name, 'main_', '') FROM sys.tables WHERE name LIKE 'main_%'))
            AND name NOT LIKE 'temp_%';

            OPEN @cursor;
            FETCH NEXT FROM @cursor INTO @tbl;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @sql = N'SELECT @cntOUT = COUNT(*) FROM [dbo].[' + @tbl + '] WHERE $PARTITION.PF_LOGTIME_YEARLY(logtime) = @P_ID';
                EXEC sp_executesql @sql, N'@P_ID INT, @cntOUT BIGINT OUTPUT', @P_ID=@PartitionID, @cntOUT=@rowcount OUTPUT;
                
                IF @rowcount > 0
                BEGIN
                    SET @tableCount = @tableCount + 1;
                    SET @totalRows = @totalRows + @rowcount;
                    
                    PRINT 'Table: ' + @tbl + ' - Rows to purge: ' + CAST(@rowcount AS VARCHAR(20));
                    
                    SET @sql = 'ALTER TABLE [dbo].[' + @tbl + '] SWITCH PARTITION ' + CAST(@PartitionID AS NVARCHAR(10)) + 
                               ' TO [dbo].[temp_' + @tbl + '] PARTITION ' + CAST(@PartitionID AS NVARCHAR(10)) + ';';
                    
                    EXEC(@sql);
                    PRINT 'Switched partition for: ' + @tbl;

                    SET @sql = 'TRUNCATE TABLE [dbo].[temp_' + @tbl + '];';
                    EXEC(@sql);
                    PRINT 'Truncated temp table for: ' + @tbl;
                END

                FETCH NEXT FROM @cursor INTO @tbl;
            END
            
            CLOSE @cursor;
            DEALLOCATE @cursor;
            
            PRINT 'Year {year} purge complete - Tables processed: ' + CAST(@tableCount AS VARCHAR(10)) + ', Total rows purged: ' + CAST(@totalRows AS VARCHAR(20));
            """
            
            cursor.execute(purge_sql)
            
            while cursor.nextset():
                pass
                
            logger.info(f"✓ Year {year} purged successfully")
        
        logger.info("✓ All years purged successfully")
        
    except pyodbc.Error as e:
        logger.error(f"SQL Server error during purge: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"Unexpected error during purge: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")

def yearly_backup():
    """Create final yearly backup after cleanup, rebuilding only fragmented indexes."""
    logger.info("="*80)
    logger.info("FINAL CLEANUP AND YEARLY BACKUP")
    logger.info("="*80)
    
    conn = None
    cursor = None
    
    try:
        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()
        
        # 1. Shrink Database
        logger.info("Shrinking database...")
        cursor.execute(f"USE [{config['database']}]; DBCC SHRINKDATABASE([{config['database']}], 10)")
        while cursor.nextset(): pass
        logger.info("✓ Database shrunk successfully")

        # 2. Identify and Drop Fragmented Indexes (> 30%)
        # logger.info("Checking for fragmented indexes (> 30%)...")
        
        # This SQL finds fragmented indexes and returns their names/stats for logging
        # then builds the DROP string.
        drop_idx_sql = f"""
        USE [{config['database']}];
        DECLARE @sql NVARCHAR(MAX) = N'';
        DECLARE @info_msg NVARCHAR(MAX) = N'';

        -- Table variable to store targets
        DECLARE @TargetIndexes TABLE (
            SchemaName SYSNAME,
            TableName SYSNAME,
            IndexName SYSNAME,
            Frag FLOAT
        );

        INSERT INTO @TargetIndexes
        SELECT 
            s.name, t.name, i.name, stats.avg_fragmentation_in_percent
        FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, 'LIMITED') AS stats
        INNER JOIN sys.tables t ON stats.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.indexes i ON i.object_id = t.object_id AND stats.index_id = i.index_id
        WHERE stats.avg_fragmentation_in_percent > 30
          AND i.is_primary_key = 0
          AND i.type_desc = 'CLUSTERED';

        -- Build the DROP commands and a status message for logging
        SELECT 
            @sql = @sql + 'DROP INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '];' + CHAR(10),
            @info_msg = @info_msg + 'Targeting: [' + TableName + '].[' + IndexName + '] - Frag: ' + CAST(CAST(Frag AS DECIMAL(5,2)) AS VARCHAR) + '%' + CHAR(10)
        FROM @TargetIndexes;

        IF @sql <> N''
        BEGIN
            PRINT @info_msg;
            EXEC sp_executesql @sql;
        END
        ELSE
        BEGIN
            PRINT 'No indexes found with fragmentation > 30%. Skipping drop/rebuild.';
        END
        """
        logger.info(f"Rebuilding indexes...")
        # cursor.execute(drop_idx_sql)
        rebuild_idx_sql = "EXEC sp_MSforeachtable 'ALTER INDEX ALL ON ? REBUILD';"
        cursor.execute(rebuild_idx_sql)
        logger.info(f"Index rebuild finished.")

        # Capture the PRINT statements from SQL for logging
        while True:
            messages = cursor.messages
            for msg in messages:
                # msg is a tuple (message, server, ...)
                logger.info(f"[SQL] {msg[1] if isinstance(msg[1], str) else msg[0]}")
            if not cursor.nextset():
                break

        # 3. Recreate Indexes
        # Note: Ensure create_indexes() only recreates what is missing or 
        # is prepared to handle "already exists" errors.

        # logger.info("Recreating indexes...")
        # create_indexes()

        # Change recovery mode to FULL
        logger.info("Changing recovery mode from SIMPLE to FULL")
        cursor.execute(f"ALTER DATABASE [{config['database']}] SET RECOVERY FULL")
        logger.info("Recovery mode changed to : FULL")

        # 4. Final Backup
        logger.info("Creating final yearly backup...")
        # Re-establishing connection for the backup operation
        if cursor: cursor.close()
        if conn: conn.close()
        
        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()
        
        yearly_bkp_path = os.path.join(
            config['target_backup_folder'], 
            f"{config['database']}_{str(REQUIRED_BACKUP_YEAR)}_Yearly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
        )
        
        logger.info(f"Backup file: {yearly_bkp_path}")
        cursor.execute(f"BACKUP DATABASE [{config['database']}] TO DISK='{yearly_bkp_path}' WITH FORMAT, COMPRESSION, STATS=10")
        
        while cursor.nextset(): pass
        
        if os.path.exists(yearly_bkp_path):
            file_size = os.path.getsize(yearly_bkp_path) / (1024 * 1024 * 1024)  # GB
            logger.info(f"✓ Yearly backup created successfully ({file_size:.2f} GB)")
        
        logger.info("="*80)
        logger.info("✓ ALL PROCESSES COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        
    except pyodbc.Error as e:
        logger.error(f"SQL Server error during final backup: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    except Exception as e:
        logger.error(f"Unexpected error during final backup: {str(e)}")
        logger.debug(traceback.format_exc())
        raise
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def from_scratch():
    start_time = time.time()
    
    try:
        logger.info("="*80)
        logger.info("DATABASE AUTOMATION SCRIPT STARTED - FROM SCRATCH")
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        logger.info(f"Configuration:")
        logger.info(f"  Server: {config['server']}")
        logger.info(f"  Database: {config['database']}")
        logger.info(f"  Source folder: {config['source_backup_folder']}")
        logger.info(f"  Target folder: {config['target_backup_folder']}")
        logger.info(f"  Restore path: {config['restore_path']}")
        logger.info(f"  Yearly Backup Year: {REQUIRED_BACKUP_YEAR}")
        logger.info("="*80)

        verify = input("Please verify the configuration before proceeding (y/n): ")

        if verify != 'y':
            return

        master_bkp_flag = input("Do you want to take Master Backup (Full & Partitioned) (y/n): ")

        # Step 1: Restore
        if not restoration(type='SCRATCH'):
            logger.error("Restoration failed. Aborting remaining steps.")
            return False

        # Step 2: Prepare
        prepare_database()

        # Step 3: Create Partitions
        create_partitions()

        # Step 4: Create Indexes
        create_indexes()

        # Step 5: Create Temp Tables
        create_temp_tables()

        # Step 6: Master Backup
        if master_bkp_flag == 'y':
            create_master_backup()

        # Step 7: Purge Years
        purge_years()

        # Step 8: Final Backup
        yearly_backup()
        
        elapsed_time = time.time() - start_time
        logger.info("="*80)
        logger.info(f"✓ SCRIPT COMPLETED SUCCESSFULLY")
        logger.info(f"Total execution time: {elapsed_time/60:.2f} minutes")
        logger.info("="*80)
        return True

    except KeyboardInterrupt:
        logger.warning("="*80)
        logger.warning("Script interrupted by user (Ctrl+C)")
        logger.warning("="*80)
        return False
        
    except Exception as e:
        logger.error("="*80)
        logger.error("❌ SCRIPT ABORTED DUE TO ERROR")
        logger.error(f"Error: {str(e)}")
        logger.error("="*80)
        logger.debug("Full traceback:")
        logger.debug(traceback.format_exc())
        return False

    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"Script execution time: {elapsed_time/60:.2f} minutes")
        logger.info("Script finished.")

def from_master():
    start_time = time.time()
    
    try:
        logger.info("="*80)
        logger.info("DATABASE AUTOMATION SCRIPT STARTED - FROM MASTER BACKUP")
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        logger.info(f"Configuration:")
        logger.info(f"  Server: {config['server']}")
        logger.info(f"  Database: {config['database']}")
        logger.info(f"  Source Master file: {config['source_master_file']}") 
        logger.info(f"  Target folder: {config['target_backup_folder']}")
        logger.info(f"  Restore path: {config['restore_path']}")
        logger.info(f"  Yearly Backup Year: {REQUIRED_BACKUP_YEAR}")
        logger.info("="*80)

        verify = input("Please verify the configuration before proceeding (y/n): ")

        if verify != 'y':
            return

        # Step 1: Restore
        if not restoration(type='MASTER'):
            logger.error("Restoration failed. Aborting remaining steps.")
            return False

        conn = pyodbc.connect(_get_connection_string(), autocommit=True, timeout=0)
        cursor = conn.cursor()

        cursor.execute(f"ALTER DATABASE [{config['database']}] SET RECOVERY SIMPLE")

        cursor.close()
        conn.close()

        # Step 7: Purge Years
        purge_years()

        # Step 8: Final Backup
        yearly_backup()
        
        elapsed_time = time.time() - start_time
        logger.info("="*80)
        logger.info(f"✓ SCRIPT COMPLETED SUCCESSFULLY")
        logger.info(f"Total execution time: {elapsed_time/60:.2f} minutes")
        logger.info("="*80)
        return True

    except KeyboardInterrupt:
        logger.warning("="*80)
        logger.warning("Script interrupted by user (Ctrl+C)")
        logger.warning("="*80)
        return False
        
    except Exception as e:
        logger.error("="*80)
        logger.error("❌ SCRIPT ABORTED DUE TO ERROR")
        logger.error(f"Error: {str(e)}")
        logger.error("="*80)
        logger.debug("Full traceback:")
        logger.debug(traceback.format_exc())
        return False

    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"Script execution time: {elapsed_time/60:.2f} minutes")
        logger.info("Script finished.")

def main():
    ch = int(input("Enter your choice:\nPress 1 for RESTORE_FROM_SCRATCH\nPress 2 for RESTORE_FROM_MASTER\n"))
    if ch == 1:
        from_scratch()
    elif ch == 2:
        from_master()
    else:
        logger.info("WRONG CHOICE ENTERED.")

if __name__ == "__main__":
    exit(0 if main() else 1)
