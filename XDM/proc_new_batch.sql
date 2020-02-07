
----------------------------------------------------------
--
-- Filename: proc_new_batch.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-01-23
--
--  Description:  Creates a new batch audit record and returns the next available BATCH_SK
--
----------------------------------------------------------


alter procedure XDM.SP_NEW_BATCH
@BATCH_DESC varchar(100),
@NEW_SK bigint output,
@RETURN_CODE integer output
as

declare @ERROR_CODE bigint


set @RETURN_CODE = -1

--- Get next BATCH_SK ---
select @new_sk = max(batch_sk) + 1 from xdm.dim_batch_audit
-- select @new_sk


--- insert new row in DIM_BATCH_AUDIT ---
begin try
   insert into xdm.dim_batch_audit values( @new_sk, @batch_desc, getdate(), null, 'R', null, null, null, null )
   set @RETURN_CODE = 0
end try
begin catch
   select error_message()
   select 'ERROR CREATING NEW BATCH'
   set @RETURN_CODE = 1
end catch


-- TEST ---
/*
select * from xdm.dim_batch_audit

declare @RET_VAL integer, @NEW_SK bigint
exec xdm.sp_new_batch 'TEST2', @NEW_SK out, @RET_VAL out
select @NEW_SK, @RET_VAL

delete xdm.dim_batch_audit where batch_sk <> -1
*/