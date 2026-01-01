/* =======================================================================================
   FROM SCRATCH – MANUAL SQL SCRIPT
   This script reproduces the FROM SCRATCH flow of your Python automation:
     1) Restore database from full + log backups
     2) Set recovery to SIMPLE and drop non‑PK clustered indexes
     3) Create partition function and scheme (yearly on logtime)
     4) Create clustered indexes on partition scheme
     5) Create temp tables for partition switching
     6) Take a "master" full backup of the partitioned database
     7) Purge unwanted years by partition switching to temp tables
     8) Shrink DB, rebuild indexes, take final yearly backup
   ======================================================================================= */

-- =========================================
-- 0. CONFIGURATION (EDIT THESE VALUES)
-- =========================================
USE master;
GO

-- Database name
DECLARE @DBName SYSNAME = N'TESTDB23';           -- TODO: change if needed

-- Data / log restore target path for MDF/LDF
DECLARE @RestorePath NVARCHAR(260) = N'D:\SQLData';  -- TODO: change to your data folder

-- FULL and LOG backup files (FROM SCRATCH path = restore from folder)
-- You must put the real generated file paths here.
DECLARE @FullBackup    NVARCHAR(260) = N'D:\BACKUP\TESTDB\YourFullBackup.bak';   -- TODO
DECLARE @LogBackup1    NVARCHAR(260) = N'D:\BACKUP\TESTDB\YourLogBackup1.trn';   -- TODO
DECLARE @LogBackup2    NVARCHAR(260) = N'D:\BACKUP\TESTDB\YourLogBackup2.trn';   -- TODO
-- Add more log backup variables as needed...
DECLARE @LastLogBackup NVARCHAR(260) = @LogBackup2;  -- TODO: set to final log backup

-- Partitioning years (must match what you want in PF_LOGTIME_YEARLY)
-- In Python: PARTITION_YEARS = [2022, 2023, 2024, 2025, 2026]
-- In pure SQL we'll just hard-code them later.

-- Year you want to KEEP for yearly backup (REQUIRED_BACKUP_YEAR[0] in Python)
DECLARE @RequiredYear INT = 2023;  -- TODO: change if needed

-- Backup target folder for master + yearly backups
DECLARE @TargetBackupFolder NVARCHAR(260) = N'E:\BACKUP';  -- TODO: change

-- File names for final backups (you can adjust naming convention)
DECLARE @MasterBackupFile NVARCHAR(260) =
    @TargetBackupFolder + N'\' + @DBName + N'_Partitioned_MASTER.bak';

DECLARE @YearlyBackupFile NVARCHAR(260) =
    @TargetBackupFolder + N'\' + @DBName + N'_' + CAST(@RequiredYear AS NVARCHAR(4)) + N'_Yearly.bak';

PRINT 'Configuration loaded.';
GO


/* =======================================================================================
   1. RESTORE DATABASE FROM FULL + LOG BACKUPS
   Equivalent to restore_database_from_folder() in the Python script (manual version).
   ======================================================================================= */

-- 1.1 Check if database exists
SELECT name,
       state_desc,
       recovery_model_desc,
       physical_database_name
FROM sys.databases
WHERE name = @DBName;
GO

-- 1.2 (Optional but recommended) Force SINGLE_USER to break connections if DB exists
--     COMMENT OUT if this is a brand-new DB restore.
DECLARE @exists INT;
SELECT @exists = COUNT(*) FROM sys.databases WHERE name = @DBName;

IF @exists = 1
BEGIN
    PRINT 'Setting database to SINGLE_USER to kill active connections...';
    EXEC (N'ALTER DATABASE [' + @DBName + N'] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;');
END;
GO

-- 1.3 Inspect FULL backup file to get logical file names
RESTORE FILELISTONLY
FROM DISK = @FullBackup;
GO
/*
   NOTE: From the result above, note:
     - LogicalName of data file, e.g. 'TESTDB23'
     - LogicalName of log file,  e.g. 'TESTDB23_log'
   Then put them into the RESTORE DATABASE command below.
*/

-- 1.4 RESTORE FULL backup WITH NORECOVERY, REPLACE and STATS
RESTORE DATABASE [TESTDB23]       -- TODO: replace logical names and DB if different
FROM DISK = N'D:\BACKUP\TESTDB\YourFullBackup.bak'     -- @FullBackup
WITH
    MOVE N'TESTDB23'     TO N'D:\SQLData\TESTDB23.mdf',      -- data logical name/path
    MOVE N'TESTDB23_log' TO N'D:\SQLData\TESTDB23_log.ldf',  -- log logical name/path
    REPLACE,
    NORECOVERY,
    STATS = 5;
GO

-- 1.5 RESTORE LOG backups IN ORDER (NO RECOVERY for all but last)
RESTORE LOG [TESTDB23]
FROM DISK = N'D:\BACKUP\TESTDB\YourLogBackup1.trn'
WITH NORECOVERY, STATS = 5;
GO

RESTORE LOG [TESTDB23]
FROM DISK = N'D:\BACKUP\TESTDB\YourLogBackup2.trn'
WITH NORECOVERY, STATS = 5;
GO

-- Repeat the above block for all intermediate log backups...

-- 1.6 Final LOG restore WITH RECOVERY
RESTORE LOG [TESTDB23]
FROM DISK = N'D:\BACKUP\TESTDB\YourLastLogBackup.trn'
WITH RECOVERY, STATS = 5;
GO

-- 1.7 Verify final state
SELECT name, state_desc, recovery_model_desc
FROM sys.databases
WHERE name = N'TESTDB23';
GO


/* =======================================================================================
   2. PREPARE DATABASE FOR PARTITIONING
      - Set RECOVERY to SIMPLE
      - Drop existing non‑PK clustered indexes
   Equivalent to prepare_database() in the Python script.
   ======================================================================================= */

USE [TESTDB23];   -- or [@DBName] via dynamic SQL if you prefer
GO

-- 2.1 Set recovery model to SIMPLE
ALTER DATABASE [TESTDB23] SET RECOVERY SIMPLE;
GO

-- 2.2 Drop all clustered indexes that are NOT primary keys
DECLARE @sql   NVARCHAR(MAX) = N'';
DECLARE @count INT = 0;

SELECT
    @sql = STRING_AGG(
        'DROP INDEX [' + i.name + '] ON [' + SCHEMA_NAME(t.schema_id) + '].[' + t.name + '];',
        CHAR(10)
    ),
    @count = COUNT(*)
FROM sys.tables t
JOIN sys.indexes i ON i.object_id = t.object_id
WHERE i.type_desc = 'CLUSTERED'
  AND i.is_primary_key = 0;

IF @sql IS NOT NULL
BEGIN
    PRINT 'Dropping ' + CAST(@count AS VARCHAR(10)) + ' clustered index(es)...';
    EXEC sp_executesql @sql;
    PRINT 'Indexes dropped successfully.';
END
ELSE
BEGIN
    PRINT 'No clustered indexes to drop.';
END;
GO


/* =======================================================================================
   3. CREATE PARTITION FUNCTION AND SCHEME
   Equivalent to create_partitions() in the Python script.
   Uses yearly boundaries for logtime.
   ======================================================================================= */

USE [TESTDB23];
GO

-- 3.1 Drop existing partition scheme/function if present
IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'PS_LOGTIME_YEARLY')
BEGIN
    PRINT 'Dropping existing partition scheme: PS_LOGTIME_YEARLY';
    DROP PARTITION SCHEME PS_LOGTIME_YEARLY;
END;
GO

IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PF_LOGTIME_YEARLY')
BEGIN
    PRINT 'Dropping existing partition function: PF_LOGTIME_YEARLY';
    DROP PARTITION FUNCTION PF_LOGTIME_YEARLY;
END;
GO

-- 3.2 Create partition function and scheme
PRINT 'Creating partition function: PF_LOGTIME_YEARLY';
CREATE PARTITION FUNCTION PF_LOGTIME_YEARLY (DATETIME)
AS RANGE RIGHT FOR VALUES
(
    '2022-01-01 00:00:00',
    '2023-01-01 00:00:00',
    '2024-01-01 00:00:00',
    '2025-01-01 00:00:00',
    '2026-01-01 00:00:00'
);
GO

PRINT 'Creating partition scheme: PS_LOGTIME_YEARLY';
CREATE PARTITION SCHEME PS_LOGTIME_YEARLY
AS PARTITION PF_LOGTIME_YEARLY
ALL TO ([PRIMARY]);
GO

PRINT 'Partition function and scheme created successfully.';
GO


/* =======================================================================================
   4. CREATE CLUSTERED INDEXES ON PARTITION SCHEME
   Equivalent to create_indexes() in the Python script.
   Targets:
     - Tables named like 'main_%'
     - Their base tables (REPLACE(name, 'main_', ''))
   All clustered on logtime ON PS_LOGTIME_YEARLY(logtime).
   ======================================================================================= */

USE [TESTDB23];
GO

DECLARE @idxSql   NVARCHAR(MAX) = N'';
DECLARE @idxCount INT = 0;

SELECT
    @idxSql = STRING_AGG(
        CAST(
            'CREATE CLUSTERED INDEX [CIX_' + t.name + '] ' +
            'ON [dbo].[' + t.name + '](logtime) ' +
            'ON PS_LOGTIME_YEARLY(logtime);'
            AS NVARCHAR(MAX)
        ),
        CHAR(10)
    ),
    @idxCount = COUNT(*)
FROM sys.tables t
WHERE
    (
        t.name LIKE 'main_%'
        OR t.name IN (
            SELECT REPLACE(name, 'main_', '')
            FROM sys.tables
            WHERE name LIKE 'main_%'
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        WHERE i.object_id = t.object_id
          AND i.type_desc = 'CLUSTERED'
    );

IF @idxSql IS NOT NULL
BEGIN
    PRINT 'Creating ' + CAST(@idxCount AS VARCHAR(10)) + ' clustered index(es) on partition scheme...';
    EXEC sp_executesql @idxSql;
    PRINT 'Indexes created successfully.';
END
ELSE
BEGIN
    PRINT 'No indexes to create.';
END;
GO

/*========================================
rebuilding indexes
=========================================*/
USE [TESTDB23];
GO

DECLARE @idxSql   NVARCHAR(MAX) = N'';
DECLARE @idxCount INT = 0;

SELECT
    @idxSql = STRING_AGG(
        CAST(
            'ALTER INDEX [' + i.name + '] ' +
            'ON [dbo].[' + t.name + '] ' +
            'REBUILD ' +
            -- Uncomment if your SQL edition supports it
            -- 'WITH (ONLINE = ON) ' +
            ';'
            AS NVARCHAR(MAX)
        ),
        CHAR(10)
    ),
    @idxCount = COUNT(*)
FROM sys.tables t
JOIN sys.indexes i
    ON i.object_id = t.object_id
   AND i.type_desc = 'CLUSTERED'
WHERE
    (
        t.name LIKE 'main_%'
        OR t.name IN (
            SELECT REPLACE(name, 'main_', '')
            FROM sys.tables
            WHERE name LIKE 'main_%'
        )
    );

IF @idxSql IS NOT NULL AND @idxCount > 0
BEGIN
    PRINT 'Rebuilding ' + CAST(@idxCount AS VARCHAR(10)) + ' clustered index(es)...';
    EXEC sp_executesql @idxSql;
    PRINT 'Indexes rebuilt successfully.';
END
ELSE
BEGIN
    PRINT 'No clustered indexes found to rebuild.';
END;
GO

/* =======================================================================================
   5. CREATE TEMP TABLES FOR PARTITION SWITCHING
   Equivalent to create_temp_tables() in the Python script.
   For each main_* (and its base) table:
     - Create dbo.temp_<TableName> with same structure
     - Put clustered index on logtime ON PS_LOGTIME_YEARLY(logtime)
   ======================================================================================= */

USE [TESTDB23];
GO

DECLARE @tbl    NVARCHAR(255);
DECLARE @curTbl CURSOR;
DECLARE @tmpCount INT = 0;

SET @curTbl = CURSOR FOR
    SELECT name
    FROM sys.tables
    WHERE (name LIKE 'main_%'
           OR name IN (
               SELECT REPLACE(name, 'main_', '')
               FROM sys.tables
               WHERE name LIKE 'main_%'
           ))
      AND name NOT LIKE 'temp_%';

OPEN @curTbl;
FETCH NEXT FROM @curTbl INTO @tbl;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @tmpCount = @tmpCount + 1;

    IF OBJECT_ID('dbo.temp_' + @tbl) IS NOT NULL
    BEGIN
        PRINT 'Dropping existing temp table: temp_' + @tbl;
        EXEC('DROP TABLE [dbo].[temp_' + @tbl + ']');
    END;

    PRINT 'Creating temp table: temp_' + @tbl;
    EXEC('SELECT * INTO [dbo].[temp_' + @tbl + '] FROM [dbo].[' + @tbl + '] WHERE 1 = 0');

    PRINT 'Creating clustered index on temp_' + @tbl;
    EXEC('CREATE CLUSTERED INDEX [CIX_temp_' + @tbl + '] ' +
         'ON [dbo].[temp_' + @tbl + '](logtime) ' +
         'ON PS_LOGTIME_YEARLY(logtime)');

    FETCH NEXT FROM @curTbl INTO @tbl;
END;

CLOSE @curTbl;
DEALLOCATE @curTbl;

PRINT 'Created ' + CAST(@tmpCount AS VARCHAR(10)) + ' temp table(s).';
GO


/* =======================================================================================
   6. CREATE MASTER BACKUP OF PARTITIONED DATABASE
   Equivalent to create_master_backup() (minus folder creation logic).
   ======================================================================================= */

BACKUP DATABASE [TESTDB23]
TO DISK = N'E:\BACKUP\TESTDB23_Partitioned_MASTER.bak'
WITH FORMAT, COMPRESSION, STATS = 10;
GO


/* =======================================================================================
   7. PURGE YEARS USING PARTITION SWITCHING
   Equivalent to purge_years() in the Python script.
   YEARS_TO_PURGE = PARTITION_YEARS - {RequiredYear}
   Below is a reusable block; run it once per year you want to purge.
   ======================================================================================= */

-- TEMPLATE: PURGE A SINGLE YEAR
-- Replace <YEAR> with actual year (e.g. 2022, 2024, 2025, 2026)
USE [TESTDB23];
GO

DECLARE @YearToPurge INT = 2022;  -- TODO: change for each year you purge
DECLARE @PartitionID INT;

SELECT @PartitionID = $PARTITION.PF_LOGTIME_YEARLY(
    CAST(@YearToPurge AS NVARCHAR(4)) + '-01-01'
);

PRINT 'Partition ID for year ' + CAST(@YearToPurge AS VARCHAR(10)) +
      ': ' + CAST(@PartitionID AS VARCHAR(10));

DECLARE @pSql       NVARCHAR(MAX);
DECLARE @pTbl       NVARCHAR(255);
DECLARE @rowcount   BIGINT;
DECLARE @totalRows  BIGINT = 0;
DECLARE @tableCount INT   = 0;
DECLARE @curPurge   CURSOR;

SET @curPurge = CURSOR FOR
    SELECT name
    FROM sys.tables
    WHERE (name LIKE 'main_%'
           OR name IN (
               SELECT REPLACE(name, 'main_', '')
               FROM sys.tables
               WHERE name LIKE 'main_%'
           ))
      AND name NOT LIKE 'temp_%';

OPEN @curPurge;
FETCH NEXT FROM @curPurge INTO @pTbl;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @pSql = N'
        SELECT @cntOUT = COUNT(*)
        FROM [dbo].[' + @pTbl + N']
        WHERE $PARTITION.PF_LOGTIME_YEARLY(logtime) = @P_ID;
    ';

    EXEC sp_executesql @pSql,
        N'@P_ID INT, @cntOUT BIGINT OUTPUT',
        @P_ID   = @PartitionID,
        @cntOUT = @rowcount OUTPUT;

    IF @rowcount > 0
    BEGIN
        SET @tableCount = @tableCount + 1;
        SET @totalRows  = @totalRows  + @rowcount;

        PRINT 'Table: ' + @pTbl + ' - Rows to purge: ' + CAST(@rowcount AS VARCHAR(20));

        SET @pSql = N'ALTER TABLE [dbo].[' + @pTbl + N'] SWITCH PARTITION ' +
                    CAST(@PartitionID AS NVARCHAR(10)) +
                    N' TO [dbo].[temp_' + @pTbl + N'] PARTITION ' +
                    CAST(@PartitionID AS NVARCHAR(10)) + N';';
        EXEC(@pSql);
        PRINT 'Switched partition for: ' + @pTbl;

        SET @pSql = N'TRUNCATE TABLE [dbo].[temp_' + @pTbl + N'];';
        EXEC(@pSql);
        PRINT 'Truncated temp table for: ' + @pTbl;
    END;

    FETCH NEXT FROM @curPurge INTO @pTbl;
END;

CLOSE @curPurge;
DEALLOCATE @curPurge;

PRINT 'Year ' + CAST(@YearToPurge AS VARCHAR(10)) +
      ' purge complete - Tables processed: ' +
      CAST(@tableCount AS VARCHAR(10)) +
      ', Total rows purged: ' +
      CAST(@totalRows AS VARCHAR(20)) + '.';
GO

/*
   Repeat the PURGE block above for each year in {2022, 2024, 2025, 2026}
   (i.e., all PARTITION_YEARS except @RequiredYear = 2023).
*/


/* =======================================================================================
   8. FINAL CLEANUP AND YEARLY BACKUP
   Equivalent to yearly_backup() in the Python script.
   Steps:
     - SHRINKDATABASE
     - Drop and recreate clustered indexes (Step 4 logic reused)
     - Take final yearly backup
   ======================================================================================= */

USE [TESTDB23];
GO

-- 8.1 Shrink database
PRINT 'Shrinking database...';
DBCC SHRINKDATABASE([TESTDB23], 10);
GO

-- 8.2 Drop clustered non‑PK indexes again (for rebuild)
DECLARE @dropSql   NVARCHAR(MAX) = N'';
DECLARE @dropCount INT = 0;

SELECT
    @dropSql = STRING_AGG(
        'DROP INDEX [' + i.name + '] ON [' + SCHEMA_NAME(t.schema_id) + '].[' + t.name + '];',
        CHAR(10)
    ),
    @dropCount = COUNT(*)
FROM sys.tables t
JOIN sys.indexes i ON i.object_id = t.object_id
WHERE i.type_desc = 'CLUSTERED'
  AND i.is_primary_key = 0;

IF @dropSql IS NOT NULL
BEGIN 
    PRINT 'Dropping ' + CAST(@dropCount AS VARCHAR(10)) + ' index(es) for rebuild...';
    EXEC sp_executesql @dropSql;
    PRINT 'Indexes dropped.';
END
ELSE
BEGIN
    PRINT 'No clustered indexes to drop.';
END;
GO

-- 8.3 Recreate clustered indexes on partition scheme (same as Step 4)
DECLARE @reIdxSql   NVARCHAR(MAX) = N'';
DECLARE @reIdxCount INT = 0;

SELECT
    @reIdxSql = STRING_AGG(
        CAST(
            'CREATE CLUSTERED INDEX [CIX_' + t.name + '] ' +
            'ON [dbo].[' + t.name + '](logtime) ' +
            'ON PS_LOGTIME_YEARLY(logtime);'
            AS NVARCHAR(MAX)
        ),
        CHAR(10)
    ),
    @reIdxCount = COUNT(*)
FROM sys.tables t
WHERE
    (
        t.name LIKE 'main_%'
        OR t.name IN (
            SELECT REPLACE(name, 'main_', '')
            FROM sys.tables
            WHERE name LIKE 'main_%'
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        WHERE i.object_id = t.object_id
          AND i.type_desc = 'CLUSTERED'
    );

IF @reIdxSql IS NOT NULL
BEGIN
    PRINT 'Recreating ' + CAST(@reIdxCount AS VARCHAR(10)) + ' clustered index(es)...';
    EXEC sp_executesql @reIdxSql;
    PRINT 'Indexes recreated successfully.';
END
ELSE
BEGIN
    PRINT 'No clustered indexes to create.';
END;
GO

-- 8.4 Final yearly backup for @RequiredYear (e.g., 2023)
PRINT 'Creating final yearly backup...';
BACKUP DATABASE [TESTDB23]
TO DISK = N'E:\BACKUP\TESTDB23_2023_Yearly.bak'
WITH FORMAT, COMPRESSION, STATS = 10;
GO

PRINT 'ALL STEPS COMPLETED SUCCESSFULLY (FROM SCRATCH FLOW).';
GO
