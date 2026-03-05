/*******************************************************************************
-- Object: [META].[SP_INS_RAW_PIP_INFO]
-- Description: Centralized Logging Procedure for ETL Pipelines.
--              Includes automated performance outlier detection.
-- Author: [Your Name]
-- Note: Re-architected for academic demonstration using synthetic metadata.
*******************************************************************************/

CREATE PROC [META].[SP_INS_RAW_PIP_INFO]  
     @PIP_NM [NVARCHAR](250)  
    ,@ORDER_KEY [nvarchar](100) 
    ,@SRC_SYSTEM_NM [nvarchar](100)
    ,@SRC_LAKECONTAINER_NM [nvarchar](100)
    ,@SRC_LAKEPATH_NM [nvarchar](200)
    ,@SRC_LAKEFILE_NM [nvarchar](100)
    ,@SRC_DB_NM [nvarchar](100)
    ,@SRC_SCHEMA_NM [nvarchar](100)
    ,@SRC_TABLE_NM [nvarchar](100)
    ,@TRGT_SYSTEM_NM [nvarchar](100)
    ,@TRGT_LAKECONTAINER_NM [nvarchar](100)
    ,@TRGT_LAKEPATH_NM [nvarchar](200)
    ,@TRGT_LAKEFILE_NM [nvarchar](100)
    ,@TRGT_DB_NM [nvarchar](100)
    ,@TRGT_SCHEMA_NM [nvarchar](100)
    ,@TRGT_TABLE_NM [nvarchar](100)
    ,@PIP_EXEC_DT [nvarchar](100)  
    ,@PIP_EXEC_HH [nvarchar](100)   
    ,@PIP_EXEC_MM [nvarchar](100)  
    ,@PARAM_DT [nvarchar](200)   
    ,@PARAM_HH [nvarchar](200)   
    ,@SCHEDULE_TP [nvarchar](100)   
    ,@NOTEBOOK_NM [nvarchar](200)  
    ,@RUN_ID [NVARCHAR](250)  
    ,@RUN_STATUS [NVARCHAR](50)  
    ,@START_TIME [DATETIME]  
    ,@END_TIME [DATETIME]  
    ,@ERROR_CD [nvarchar](100)
    ,@ERROR_MSG [nvarchar](max)
    ,@INPUT_TP [VARCHAR](100)
    ,@PARTITION_KEY [nvarchar](1000)
    ,@SRC_LAKE_NM  [nvarchar](200)
    ,@TRGT_LAKE_NM  [nvarchar](200) 
    ,@SRC_RECORD_CNT BIGINT
    ,@TRGT_RECORD_CNT BIGINT
AS   
BEGIN  
    SET NOCOUNT ON; -- Performance optimization for stored procedures

    DECLARE @SUM_DURATION DECIMAL(15,2);
    DECLARE @CNT_DURATION INT;
    DECLARE @AVG_LOOKBACK INT = 5;                        -- Window size for outlier detection
    DECLARE @THRESHOLD_PER INT = 15;                      -- Modified threshold (%) to differentiate from origin
    DECLARE @OUTLIER_MSG NVARCHAR(200) = 'Performance Anomaly Detected'; 

    -- Get historical average duration for outlier analysis
    WITH ExecutionHistory AS (
        SELECT A.DURATION
             , ROW_NUMBER() OVER (PARTITION BY A.[ORDER_KEY] ORDER BY A.[START_TIME] DESC) AS ROWNUM
          FROM [META].[LOG_EXECUTION] A -- Renamed from LOG_INV (Safe)
         WHERE [ORDER_KEY] = @ORDER_KEY
           AND [RUN_STATUS] = 'Success' -- Only compare with successful runs
    )
    SELECT @SUM_DURATION = SUM(DURATION)
         , @CNT_DURATION = COUNT(*)
      FROM ExecutionHistory
     WHERE ROWNUM <= @AVG_LOOKBACK + 1 AND ROWNUM <> 1;
     
    -- 1. START LOGGING (If EndTime is null, it's a new entry)
    IF @END_TIME IS NULL  
    BEGIN  
        INSERT INTO [META].[LOG_EXECUTION] (  
             [PIP_NM], [ORDER_KEY], [SRC_SYSTEM_NM], [SRC_LAKECONTAINER_NM], [SRC_LAKEPATH_NM], [SRC_LAKEFILE_NM],
             [SRC_DB_NM], [SRC_SCHEMA_NM], [SRC_TABLE_NM], [TRGT_SYSTEM_NM], [TRGT_LAKECONTAINER_NM], [TRGT_LAKEPATH_NM],
             [TRGT_LAKEFILE_NM], [TRGT_DB_NM], [TRGT_SCHEMA_NM], [TRGT_TABLE_NM], [PIP_EXEC_DT], [PIP_EXEC_HH],
             [PIP_EXEC_MM], [PARAM_DT], [PARAM_HH], [SCHEDULE_TP], [NOTEBOOK_NM], [RUN_ID], [RUN_STATUS],
             [START_TIME], [END_TIME], [ERROR_CD], [ERROR_MSG], [PARTITION_KEY], [SRC_LAKE_NM], [TRGT_LAKE_NM],
             [SRC_RECORD_CNT], [TRGT_RECORD_CNT], [DURATION], [AVG_DURATION], [OUTLIER_DESC], [INPUT_TP]
        )                                     
        VALUES (  
             @PIP_NM, @ORDER_KEY, @SRC_SYSTEM_NM, @SRC_LAKECONTAINER_NM, @SRC_LAKEPATH_NM, @SRC_LAKEFILE_NM,
             @SRC_DB_NM, @SRC_SCHEMA_NM, @SRC_TABLE_NM, @TRGT_SYSTEM_NM, @TRGT_LAKECONTAINER_NM, @TRGT_LAKEPATH_NM,
             @TRGT_LAKEFILE_NM, @TRGT_DB_NM, @TRGT_SCHEMA_NM, @TRGT_TABLE_NM, @PIP_EXEC_DT, @PIP_EXEC_HH,
             @PIP_EXEC_MM, @PARAM_DT, @PARAM_HH, @SCHEDULE_TP, @NOTEBOOK_NM, @RUN_ID, @RUN_STATUS,
             @START_TIME, @END_TIME, @ERROR_CD, @ERROR_MSG, @PARTITION_KEY, @SRC_LAKE_NM, @TRGT_LAKE_NM,
             @SRC_RECORD_CNT, NULL, NULL, NULL, NULL, @INPUT_TP
        );  
    END  

    -- 2. UPDATE LOGGING ON COMPLETION
    ELSE IF @ORDER_KEY IS NOT NULL AND @END_TIME IS NOT NULL  
    BEGIN  
        UPDATE [META].[LOG_EXECUTION]  
           SET [RUN_STATUS] = @RUN_STATUS
             , [END_TIME] = @END_TIME  
             , [ERROR_CD] = @ERROR_CD
             , [ERROR_MSG] = @ERROR_MSG
             , [TRGT_RECORD_CNT] = @TRGT_RECORD_CNT
             , [DURATION] = DATEDIFF(SECOND, [START_TIME], @END_TIME)
             , [AVG_DURATION] = CASE WHEN @CNT_DURATION < @AVG_LOOKBACK THEN NULL ELSE @SUM_DURATION/@CNT_DURATION END
             , [OUTLIER_DESC] = CASE WHEN @CNT_DURATION < @AVG_LOOKBACK 
                                      OR ABS((@SUM_DURATION/@CNT_DURATION) - DATEDIFF(SECOND, [START_TIME], @END_TIME)) 
                                         <= (@THRESHOLD_PER/100.0) * (@SUM_DURATION/@CNT_DURATION)
                                     THEN NULL ELSE @OUTLIER_MSG END
         WHERE [RUN_ID] = @RUN_ID  
           AND [ORDER_KEY] = @ORDER_KEY;

        -- UPDATE PIPELINE MASTER STATUS
        UPDATE [META].[PIPELINE_MASTER] -- Renamed from ORDER_INV (Safe)
           SET [JOB_STATUS] = @RUN_STATUS
             , [LAST_UPDATE_TIMESTAMP] = SYSDATETIMEOFFSET() AT TIME ZONE 'Korea Standard Time'
         WHERE [ORDER_KEY] = @ORDER_KEY
           AND [PARAM_DT] = @PARAM_DT;
    END

    -- 3. HOUSEKEEPING: AUTO-DELETE LOGS OLDER THAN 14 DAYS
    DELETE FROM [META].[LOG_EXECUTION]
    WHERE [START_TIME] <= DATEADD(DAY, -14, GETDATE());

END
