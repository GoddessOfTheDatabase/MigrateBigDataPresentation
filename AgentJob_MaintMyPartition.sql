USE [msdb]
GO

/****** Object:  Job [MaintMyPartitionsPartition]    Script Date: 6/5/2019 10:32:19 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 6/5/2019 10:32:19 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'MaintMyPartitionsPartition', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Execute SP to check for next quarter''s Read Write partition file group, and create it if it doesn''t exist.  Job is expected to run every 2 months to prepare 1 month in advance the next quarter''s partition and file.  Previous -2 quarter partition is marked Read Only', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'Dev-OnCall', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Check Replica Designation]    Script Date: 6/5/2019 10:32:19 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Check Replica Designation', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=4, 
		@on_success_step_id=2, 
		@on_fail_action=1, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if exists

(
	
	SELECT 1
	FROM sys.dm_hadr_database_replica_states repl

		JOIN sys.databases d ON d.database_id = repl.database_id

	WHERE d.Name = ''MyPartitions_JULIE16''

)

begin
	If sys.fn_hadr_is_primary_replica ( ''MyPartitions_JULIE16'' ) <> 1
	BEGIN
	RAISERROR(''This is not the primary replica'', 16, 1)
	END
	-- If this is the primary replica, continue to next step
end', 
		@database_name=N'master', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [CheckDateDBPartition]    Script Date: 6/5/2019 10:32:19 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'CheckDateDBPartition', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=4, 
		@on_success_step_id=3, 
		@on_fail_action=4, 
		@on_fail_step_id=4, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'print ''Executing sp msdb.[dbo]CheckDateDBPartition''
	EXECUTE msdb.[dbo].MaintCheckDateDBPartition
	@PartitionDBName = ''MyPartitions'',	
	@PartitionDBEnv = ''JULIE16'', 
	@DBFileCd = ''MP''', 
		@database_name=N'MyPartitions_JULIE16', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [MaintUserActionPartition]    Script Date: 6/5/2019 10:32:19 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'MaintUserActionPartition', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=4, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'print ''Executing sp msdb.[dbo]MaintDBPartition''
 EXECUTE msdb.[dbo].MaintDBPartition 
	@PartitionDBName = ''MyPartitions'',	
	@PartitionDBEnv = ''JULIE16'', 
	@DBFileCd = ''MP''', 
		@database_name=N'MyPartitions_JULIE16', 
		@flags=12
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [EmailErrorsFormatted]    Script Date: 6/5/2019 10:32:19 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'EmailErrorsFormatted', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @JobID binary(16)
DECLARE @JobName nvarchar(256)  
SELECT @JobName = ''MaintMyPartitionsPartition'' 
DECLARE @subject nvarchar(max) = ''Prod Sev: '' +  @Severity + '' ''+ @@Servername + '' '' + @JobName;
        DECLARE @body nvarchar(max) = ''TestEmailOnFail Job Failed'' 
            + CHAR(10) + CHAR(13) + ''Error Number:  '' + CAST(ERROR_NUMBER() AS nvarchar(max))
            + CHAR(10) + CHAR(13) + ''Error Message: '' + ERROR_MESSAGE();
        DECLARE @to nvarchar(max) = ''AnyName@hotmail.com'';
        DECLARE @profile_name sysname = ''AnyName_Profile'';
        EXEC msdb.dbo.sp_send_dbmail @profile_name = @profile_name,
            @recipients = @to, @subject = @subject, @body = @body,@query_result_no_padding = 1
	,@query_result_header = 0;', 
		@database_name=N'master', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


