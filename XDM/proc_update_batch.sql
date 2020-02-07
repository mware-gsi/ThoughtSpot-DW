
----------------------------------------------------------
--
-- Filename: proc_update_batch.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-01-24
--
--  Description:  Updates the batch audit record for the supplied BATCH_SK
--
----------------------------------------------------------

-- drop procedure XDM.SP_UPDATE_BATCH

alter procedure XDM.SP_UPDATE_BATCH
@BATCH_SK bigint,
@STATUS_CODE char(1),
@INSERT_COUNT bigint,
@UPDATE_COUNT bigint,
@DELETE_COUNT bigint,
@MESSAGE varchar(500),
@RETURN_CODE integer output
as

declare @ERROR_CODE bigint

set @RETURN_CODE = -1


--- update row in DIM_BATCH_AUDIT corresponding to supplied BATCH_SK ---
begin try
   update xdm.dim_batch_audit 
   set status_code      = @STATUS_CODE,
       end_dt           = getdate(),
       row_insert_count = @INSERT_COUNT,
	   row_update_count = @UPDATE_COUNT,
	   row_delete_count = @DELETE_COUNT,
	   [message]        = @MESSAGE
   where batch_sk = @BATCH_SK

   set @RETURN_CODE = 0
end try
begin catch
   select error_message()
   select 'ERROR UPDATING BATCH RECORD - ' + cast(@BATCH_SK as varchar(12))
   set @RETURN_CODE = 1
end catch


-- TEST ---
/*
select * from xdm.dim_batch_audit

declare @RET_VAL integer
exec xdm.sp_update_batch 0, 'S', 55, 0, 0, '', @RET_VAL out
select @RET_VAL

delete xdm.dim_batch_audit where batch_sk <> -1
*/