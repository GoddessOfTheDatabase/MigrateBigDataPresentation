USE [msdb]
GO
DROP PROCEDURE MaintCheckDateDBPartition
GO

USE [msdb]
GO

/****** Object:  StoredProcedure [dbo].[MaintCheckDateDBPartition]    Script Date: 5/28/2019 10:27:56 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- EXECUTE MaintCheckDateDBPartition @PartitionDBName = 'UserActions',	@PartitionDBEnv = 'QAPerfX', @DBFileCd = 'UA'

CREATE PROCEDURE [dbo].[MaintCheckDateDBPartition]
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
Declare @FullDBName Varchar(60)
SELECT @FullDBName =  @DBName + '_'+ @env
Declare @errormessage nvarchar(2000)
DECLARE @PartitionYear char(4)
SELECT @PartitionYear = DATEPART(year, getdate()) 
DECLARE @NewQFileYear varchar(3)
DECLARE @TotalQFileYear char(1)
DECLARE @CurrentQuarter char(1)
SELECT @CurrentQuarter = FLOOR(((12 + MONTH(GETDATE()) - 1) % 12) / 3 ) + 1 
PRINT  @CurrentQuarter
DECLARE @NextQuarter char(1)
SELECT @NextQuarter = @CurrentQuarter + 1
--PRINT  @NextQuarter
DECLARE @FileYearFormat char(6)
SELECT @FileYearFormat = @FileCd + @PartitionYear + '_Q'
SELECT @TotalQFileYear = COUNT(*) from sys.master_files where DB_NAME(database_id) = @FullDBName and 
	name like @DBName + '_' + @FileYearFormat + '%'
--PRINT @TotalQFileYear
DECLARE @NewQFileExists char(1)
SELECT @NewQFileExists = 'N'
SELECT @NewQFileExists = 'Y' from sys.master_files where DB_NAME(database_id) = @FullDBName and 
	name = @DBName + '_' + @FileYearFormat + '_Q' + @NextQuarter 
--PRINT @CorrectDBParms
--PRINT @NewQFileExists 

If @TotalQFileYear = 4
	BEGIN
		SELECT @NewQFileYear = 1
		SELECT @PartitionYear =  Datepart(year, getdate())+ 1
		SELECT @FileYearFormat = @FileCd + @PartitionYear + '_Q'
	END
ELSE
If @TotalQFileYear < 4 
	BEGIN
		SELECT @NewQFileYear = @TotalQFileYear + 1
	END
ELSE
If @TotalQFileYear > 4
	BEGIN
		SELECT @errormessage = 'More Quarter files than 4. There is a problem related to the partition data files.  Research database partition files, Manually correct the error and rerun.'
		RAISERROR (@errormessage, 18, 1);
	END

IF  GETDATE() > DATEADD(dd, -45, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + 1, 0))  --45 days prior to the last day of the current quarter, roughly 6 weeks
	and GETDATE() < DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + 1, 0))  --last day of the current quarter
	BEGIN
		IF @NewQFileExists = 'N'
		BEGIN
			PRINT 'time to create the next quarter file. Going on to next step.'
		END
		ELSE 
		BEGIN
			PRINT 'New Quarter File Already exists, no action required at this time'
		SELECT @errormessage = 'New Quarter File Already exists, no action required at this time.'
		EXEC dbo.sp_stop_job  N'MaintUserActionsPartition'
		--RAISERROR (@errormessage, 18, 1);
		END
	END
ELSE
	BEGIN
		PRINT 'Too Early to create a new quarter file'
		SELECT @errormessage = 'Too Early to create a new quarter file. No action required at this time.'
		EXEC dbo.sp_stop_job  N'MaintUserActionsPartition'    
		
		--RAISERROR (@errormessage, 18, 1);
	END
END
GO


