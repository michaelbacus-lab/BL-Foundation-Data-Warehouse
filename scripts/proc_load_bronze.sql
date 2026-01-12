/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

	PRINT '-------------';
	BULK INSERT bronze.crm_cust_amortization
	FROM 'C:\Users\RAHUR\Desktop\BL Foundation DWH\datasets\source_crm\crm_cust_amortization.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_contracts';
		TRUNCATE TABLE bronze.crm_cust_contracts;

		PRINT '>> Inserting Table: bronze.crm_cust_contracts';
		BULK INSERT bronze.crm_cust_contracts
		FROM 'C:\Users\RAHUR\Desktop\BL Foundation DWH\datasets\source_crm\crm_cust_contracts.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '----------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_processing_fee';
		TRUNCATE TABLE bronze.crm_cust_processing_fee;
		PRINT '>> Inserting Table: bronze.crm_cust_processing_fee';

	BULK INSERT bronze.crm_cust_processing_fee
	FROM 'C:\Users\RAHUR\Desktop\BL Foundation DWH\datasets\source_crm\crm_cust_processing_fee.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '----------------------';

		PRINT '>> Truncating Table: bronze.crm_membership_fee';
		PRINT '>> Inserting Table: bronze.crm_membership_fee';
		
		TRUNCATE TABLE bronze.crm_membership_fee;
	BULK INSERT bronze.crm_membership_fee
	FROM 'C:\Users\RAHUR\Desktop\BL Foundation DWH\datasets\source_crm\crm_membership_fee.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

		SET @end_time = GETDATE();
		PRINT '>> Laod Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '----------------------';

	PRINT '--------------------------------------';
	PRINT 'Loading CRM Tables'
	PRINT '--------------------------------------';

	PRINT '>> Truncating Table: bronze.erp_emp_accounts';
	PRINT '>> Inserting Table: bronze.erp_emp_accounts';
		
		TRUNCATE TABLE bronze.erp_emp_accounts;
	BULK INSERT bronze.erp_emp_accounts
	FROM 'C:\Users\RAHUR\Desktop\BL Foundation DWH\datasets\source_erp\erp_emp_accounts.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

		SET @end_time = GETDATE();
		PRINT '>> Laod Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '----------------------';

	SET @end_time = GETDATE();		
	PRINT '>> Truncating Table: bronze.erp_land_owners';
	TRUNCATE TABLE bronze.erp_land_owners;
	PRINT '>> Inserting Table: bronze.erp_land_owners';

	BULK INSERT bronze.erp_land_owners
	FROM 'C:\Users\RAHUR\Desktop\BL Foundation DWH\datasets\source_erp\erp_land_owners.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT '>> Laod Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
	PRINT '----------------------';
	PRINT '>> Truncating Table: bronze.erp_lots';
	TRUNCATE TABLE bronze.erp_lots;
	PRINT '>> Inserting Table: bronze.erp_lots';

	BULK INSERT bronze.erp_lots
	FROM 'C:\Users\RAHUR\Desktop\BL Foundation DWH\datasets\source_erp\erp_lots.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

		SET @end_time = GETDATE();
		PRINT '>> Laod Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '----------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=================================';
		PRINT 'Loading Bronze Layer is completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + 'seconds';
		PRINT '=================================';
		END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH

	END
