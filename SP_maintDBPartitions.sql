drop procedure dbo.MaintDBPartition
GO

USE [msdb]
GO

/****** Object:  StoredProcedure [dbo].[MaintDBPartition]    Script Date: 5/21/2019 8:31:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- EXECUTE msdb.[dbo].MaintDbPartition @PartitionDBName = 'MyActions',	@PartitionDBEnv = 'JULIE16', @DBFileCd = 'MP'
CREATE PROCEDURE [dbo].[MaintDBPartition]
	@PartitionDBName Varchar(50),
	@PartitionDBEnv Varchar(8),
	@DBFileCd Varchar(4)
AS
BEGIN

DECLARE @DBName Varchar(50)
DECLARE @Env Varchar(8)
DECLARE @FileCd Varchar(4)
	SELECT @DBName = @PartitionDBName
	SELECT @Env = @PartitionDBEnv
	SELECT @FileCd = @DBFileCd
DECLARE @errormessage VARCHAR(255) 
DECLARE @FullDBName Varchar(60)
SELECT @FullDBName =  @DBName + '_'+ @env
--SELECT @FullDBName
DECLARE @FileYear Nvarchar(4)
DECLARE @FileYearFormat Nvarchar(6)
SELECT @FileYear = cast(YEAR(getdate()) as Nvarchar(4))
--SELECT @FileYearFormat = 'UA'+ @FileYear
SELECT @FileYearFormat = @FileCd + @FileYear
--SELECT @FileYearFormat
DECLARE @CorrectDBParms tinyint
SELECT @CorrectDBParms = COUNT(*) from sys.master_files where DB_NAME(database_id) = @FullDBName and name like @DBName + '_' + @FileYearFormat + '_%' 
--PRINT @CorrectDBParms

IF @CorrectDBParms = 0
	BEGIN
		SELECT @errormessage = 'The Database has no partition files that begin with the DB partition file code.  Pass in the correct code to parameter DBFILECD and rerun.'
		RAISERROR (@errormessage, 18, 1);
	END 
DECLARE @DatafileDir Varchar(4000)
exec @DatafileDir =  master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',N'Software\Microsoft\MSSQLServer\MSSQLServer',N'DefaultData', @DatafileDir output
DECLARE @FileName varchar(4000)
DECLARE @MaxQFileYear char(1)
DECLARE @NewQFileYear tinyint
DECLARE @TotalQFileYear tinyint
SELECT @TotalQFileYear = COUNT(*) FROM sys.master_files WHERE DB_NAME(database_id) = @FullDBName and name LIKE '%' + @FileYearFormat + '%'
SELECT @MaxQFileYear = COUNT(*) FROM sys.master_files WHERE DB_NAME(database_id) = @FullDBName and 
	is_read_only = 0 and name LIKE '%' + @FileYearFormat + '%'
--PRINT @TotalQFileYear
--print @MaxQFileYear
DECLARE @FileSize int
DECLARE @FileGrowth int
DECLARE @MaxFileSize char(9)
SELECT @FileSize = size , @FileGrowth = growth, @MaxFileSize = 'UNLIMITED' from sys.master_files
	WHERE DB_NAME(database_id) = @FullDBName and name = @FileYearFormat + '_Q'+@MaxQFileYear
DECLARE @FileSizeC varchar(8)
DECLARE @FileGrowthC varchar(8)
SELECT @FileSizeC = cast(@FileSize AS Varchar(8))
SELECT @FileGrowthC = cast(@FileGrowth AS Varchar(8))
--SELECT * from sys.master_files where DB_NAME(database_id) = @FullDBName and name like @FileYearFormat + '_%'
DECLARE @FileYearFormatQtrFormat varchar(25)
IF @MaxQFileYear > '4'
	BEGIN
		SELECT @errormessage = 'Quarterly partitioned table has too many files for a year created. This condition needs to be researched why and manually corrected.'
		RAISERROR (@errormessage, 18, 1);
	END
If @MaxQFileYear IN ('1','2','3')
	BEGIN
		SELECT @NewQFileYear =  cast(@MaxQFileYear as tinyint) + 1
		SELECT @FileYearFormatQtrFormat = @FileYearFormat + '_Q' + cast(@NewQFileYear AS CHAR(1))
		SELECT @FileName = @DatafileDir + '\' + @FullDBName + '_' + @DBName + '_' + @FileYearFormatQtrFormat + '.ndf'
	END
Else
If @MaxQFileYear = '4'
	BEGIN
		Declare @NewFileYear int
		SELECT @NewFileYear = YEAR(getdate()) + 1
		SELECT @FileYearFormatQtrFormat = @NewFileYear + '_Q' + cast(@NewQFileYear AS CHAR(1))
		SELECT @FileName = @DatafileDir + '\' + @FullDBName + '_' + @DBName + '_' + @FileYearFormatQtrFormat + '.ndf'
	END
--Now, Create the New FileGroup
Declare @FileGroupName nvarchar(50)
SELECT @FileGroupName = @DBName + '_' + @FileYearFormatQtrFormat
Declare @FileGroupSQL nvarchar(300)
SELECT @FileGroupSQL = 'USE ' + @FullDBName + ' ALTER DATABASE '+ @FullDBName + ' ADD FILEGROUP ' + @FileGroupName + ';'
BEGIN TRY
	EXEC sp_executesql @FileGroupSQL
	Declare @FileGroupFileNameSQL nvarchar(2000)
	SELECT @FileGroupFileNameSQL = 'USE ' + @FullDBName + ' ALTER DATABASE '+ @FullDBName +
	' ADD FILE ( Name = ' + @DBName + '_' + @FileYearFormatQtrFormat + ', 
	FILENAME = ''' + @FileName + ''', SIZE = ' + @FileSizeC + ', MAXSIZE = ' + @MaxFileSize + ', FILEGROWTH = ' +  @FileGrowthC
	+ ' ) TO FILEGROUP ' + @FileGroupName + 'XYZ' 
	EXEC sp_executesql @FileGroupFileNameSQL  
END TRY
BEGIN CATCH
	SELECT @errormessage = 'FileGroup + FileName failed to be added. Check the log for the specific error, Drop the new FileGroup if it exists. Manually correct the error and rerun.'
	RAISERROR (@errormessage, 18, 1);
END CATCH
--Get the name of the Partition Scheme and add the new File Group with file AS the Next Used
Declare @PartitionSchemeName nvarchar(30)
Declare @PartitionSchemeNameSQL nvarchar(2000)
SELECT @PartitionSchemeNameSQL = 'USE ' + @FullDBName + ' SELECT @PartitionSchemeName = name from sys.partition_schemes'
EXEC sp_executesql @PartitionSchemeNameSQL, N'@PartitionSchemeName nvarchar(30) OUTPUT', @PartitionSchemeName OUTPUT
IF @PartitionSchemeName IS NULL
	BEGIN
	  SELECT @errormessage = 'No Partition Scheme Found for this Database! Agent Job/Stored Proc is only for Partitioned Databases. Drop Filegroup created here and do not rerun with same database name!'
	  RAISERROR (@errormessage, 18, 1);
	END
Declare @PartitionSchemeSQL nvarchar(2000)
SELECT @PartitionSchemeSQL = ' USE ' + @FullDBName + ' ALTER PARTITION SCHEME '+ @PartitionSchemeName +
' NEXT USED ' + @FileGroupName
BEGIN TRY
	exec sp_executesql @PartitionSchemeSQL  
END TRY
BEGIN CATCH
	SELECT @errormessage =  'Could NOT alter Partition Scheme. Check the log for the specific error, Drop the new Filegroup, Correct Error, Rerun the job.'
	RAISERROR (@errormessage, 18, 1);
END CATCH
--Split the current partition range
Declare @PartitionFunctionName nvarchar(30)
Declare @PartitionFunctionNameSQL nvarchar(2000)
SELECT @PartitionFunctionNameSQL = 'USE ' + @FullDBName + ' SELECT @PartitionFunctionName = name from sys.partition_functions'
EXEC sp_executesql @PartitionFunctionNameSQL, N'@PartitionFunctionName nvarchar(30) OUTPUT', @PartitionFunctionName OUTPUT
IF @PartitionFunctionName IS NULL
	BEGIN
		SELECT @errormessage = 'No Partition Function Found for this Database! Agent Job/Stored Proc is only for Partitioned Databases.  Alter Partition Scheme to remove newest Filegroup created here, Drop Filegroup and do not rerun with same database name!'
		RAISERROR (@errormessage, 18, 1);
	END
Declare @MaxPartitionRangeValue varchar(23)
SELECT @MaxPartitionRangeValue = case @NewQFileYear 
	WHEN '1' THEN @FileYear + '-01-01T00:00:00.000'
	WHEN '2' THEN @FileYear + '-04-01T00:00:00.000'
	WHEN '3' THEN @FileYear + '-07-01T00:00:00.000'
	WHEN '4' THEN @FileYear + '-10-01T00:00:00.000'
	ELSE 
		Null
END 
IF @MaxPartitionRangeValue IS NULL
	BEGIN
		SELECT @errormessage =  'Wrong Quarter specified. Cannot create another quarter for database partition.  Check existing partitions, correct and rerun Agent Job.'
		RAISERROR (@errormessage, 18, 1)
	END	 
Declare @SplitPartitionRangeSQL nvarchar(2000)
SELECT @SplitPartitionRangeSQL = 'USE ' + @FullDBName + '
ALTER PARTITION FUNCTION ' +  @PartitionFunctionName +' ( ) 
SPLIT RANGE (''' + @MaxPartitionRangeValue +''')'
BEGIN TRY
	exec sp_executesql @SplitPartitionRangeSQL  
END TRY
BEGIN CATCH
	SELECT @errormessage =  'Split Partition Range Failed! Check the log for the specific error and correct error. Alter Partition Scheme to remove newest Filegroup created here, Drop Filegroup and rerun the Agent Job.'
	RAISERROR (@errormessage, 18, 1);
END CATCH
END

GO


