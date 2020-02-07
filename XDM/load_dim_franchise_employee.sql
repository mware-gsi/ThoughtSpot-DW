
---------------------------------------------------------------------------------------
--
-- Filename: load_dim_franchise_employee.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-04
--
--  Description:  Loads the DIM_FRANCHISE_EMPLOYEE table and checks for updates of the source table
--
---------------------------------------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_FRANCHISE_EMPLOYEE]
( 
	[FRANCHISE_EMPLOYEE_SK] bigint  NOT NULL ,
	[PERSON_SK]          bigint  NOT NULL ,
	[SCHOOL_SK]          bigint  NOT NULL ,
	[FMS_EMPLOYEE_ID]    bigint  NOT NULL,
	[JOB_TITLE]          varchar(100)  NULL ,
	[ROW_EFFECTIVE_DATE] date  NOT NULL ,
	[ROW_EXPIRATION_DATE] date  NOT NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NOT NULL ,
	[SRC_SYSTEM_CODE]    varchar(10)  NOT NULL ,
	[INSERT_BATCH_SK]    bigint  NOT NULL ,
	[UPDATE_BATCH_SK]    bigint  NOT NULL 
)
go

ALTER TABLE [XDM].[DIM_FRANCHISE_EMPLOYEE]
	ADD CONSTRAINT [XPKDIM_FRANCHISE_EMPLOYEE] PRIMARY KEY  CLUSTERED ([FRANCHISE_EMPLOYEE_SK] ASC)
go
*/

--  drop table [xdm].[DIM_FRANCHISE_EMPLOYEE]
--  truncate table [xdm].[DIM_FRANCHISE_EMPLOYEE]
--  delete [xdm].[DIM_FRANCHISE_EMPLOYEE] where franchise_employee_sk <> -1
--  select * from [xdm].[DIM_FRANCHISE_EMPLOYEE]

--  insert into xdm.dim_franchise_employee values (-1, -1, -1, -1, 'DEFAULT FRANCHISE EMPLOYEE', '1900-01-01', '2999-12-31','', '', -1, -1 )


-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer = 0
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK ---
exec xdm.sp_new_batch 'LOAD: DIM_FRANCHISE_EMPLOYEE', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--------------------------------------
--- load a temporary staging table ---
--------------------------------------

begin try

   select p.person_sk,
	      ds.school_sk,
          e.id                                        as FMS_EMPLOYEE_ID,
		  e.jobtitle                                  as JOB_TITLE,
          --cast( p.date_of_birth as date )             as DATE_OF_BIRTH,
          cast('1900-01-01' as date)                  as ROW_EFFECTIVE_DATE,
          cast('2999-12-31' as date)                  as ROW_EXPIRATION_DATE,
          replace(system_user, 'GODDARDSYSTEMS\', '') as LAST_UPDATE_USER,
          'FMS'                                       as SRC_SYSTEM_CODE,
          -1                                          as INSERT_BATCH_SK,
   	      -1                                          as UPDATE_BATCH_SK
   into #TMP_DIM_FRANCHISE_EMPLOYEE
   from franchisemanagement.dbo.employee as e
   join ( select max(franchise_employee_sk) as MAX_SK from poc.dim_franchise_employee ) as sk
     on 1 = 1
   join poc.dim_person as p
     on e.id = p.fms_person_id
   join franchisemanagement.dbo.school as s
     on e.schoolid = s.id
   join poc.dim_school as ds
     on cast( s.number as integer ) = cast( ds.school_number as integer )

   --  select * from #TMP_DIM_FRANCHISE_EMPLOYEE
   --  drop table #TMP_DIM_FRANCHISE_EMPLOYEE

end try
begin catch

   set @MSG = 'STAGE: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch


----------------------------------------------------------------------
--- set BATCH_SK for records in temp table not found in DIM (NEW)  ---
----------------------------------------------------------------------

begin try

   update #TMP_DIM_FRANCHISE_EMPLOYEE
   set insert_batch_sk = @BATCH_SK
   where not exists ( select 1 from xdm.dim_franchise_employee as dfe where dfe.fms_employee_id =  #TMP_DIM_FRANCHISE_EMPLOYEE.fms_employee_id )

   set @INS_COUNT = @@ROWCOUNT

end try
begin catch

   set @MSG = 'NEW: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch


----------------------------------------------------------------------------------------------------------------------------------
--- set BATCH_SK and effective date for records in temp table that have changed from the corresponding key in the DIM (UPDATE) ---
----------------------------------------------------------------------------------------------------------------------------------

begin try

   update #TMP_DIM_FRANCHISE_EMPLOYEE
   set row_effective_date = cast( getdate() as date),
       insert_batch_sk = @BATCH_SK
   from #TMP_DIM_FRANCHISE_EMPLOYEE as tdfe
   join xdm.dim_franchise_employee as dfe
     on tdfe.fms_employee_id = dfe.fms_employee_id
	and dfe.row_expiration_date = '2999-12-31'
   where tdfe.person_sk     <> dfe.person_sk
	  or tdfe.school_sk     <> dfe.school_sk
	  or tdfe.job_title     <> dfe.job_title
	  --or tdfe.date_of_birth <> dfe.date_of_birth

     
   set @UPD_COUNT = @@ROWCOUNT

   --  update #tmp_dim_franchise_employee set job_title = '#1 TEACHER' where fms_employee_id = 10107264320
end try
begin catch

   set @MSG = 'UPDATE: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch


-------------------------------------------------------------------------------------------------------------------------------
--- set expiration date for records in DIM that are different from the corresponding key in the temp table (UPDATE-EXPIRE)  ---
-------------------------------------------------------------------------------------------------------------------------------

begin try  

  update xdm.dim_franchise_employee
  set xdm.dim_franchise_employee.row_expiration_date = cast( dateadd(day, -1,  getdate()) as date ),
      xdm.dim_franchise_employee.update_batch_sk = @batch_sk
  from xdm.dim_franchise_employee as dfe
  join #TMP_DIM_FRANCHISE_EMPLOYEE as tdfe
    on dfe.fms_employee_id = tdfe.fms_employee_id
   and dfe.row_expiration_date = '2999-12-31'
   and tdfe.insert_batch_sk <> -1
      
end try
begin catch

   set @MSG = 'UPDATE-EXPIRE: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch


--------------------------------------------------------------------------------------------------------
--- set expiration date for records in DIM with a key NOT FOUND in the temp table (DELETE - EXPIRE)  ---
--------------------------------------------------------------------------------------------------------

begin try  

  update xdm.dim_franchise_employee
  set xdm.dim_franchise_employee.row_expiration_date = cast( dateadd(day, -1,  getdate()) as date ),
      xdm.dim_franchise_employee.update_batch_sk = @batch_sk
  from xdm.dim_franchise_employee as dfe
  where not exists ( select 1 from #TMP_DIM_FRANCHISE_EMPLOYEE as tdfe where dfe.fms_employee_id = tdfe.fms_employee_id )
    and dfe.franchise_employee_sk <> -1
       
   set @DEL_COUNT = @@ROWCOUNT

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = 'DELETE: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch
--  delete from #tmp_dim_franchise_employee where fms_employee_id = 10104934342


----------------------------------------------------------------------------
--- Insert new/updated records from staging table into the DIM (INSERT)  ---
----------------------------------------------------------------------------

begin try  

   insert into xdm.dim_franchise_employee
   select sk.max_sk + row_number() over (order by t.fms_employee_id) as FRANCHISE_EMPLOYEE_SK, t.* 
   from #TMP_DIM_FRANCHISE_EMPLOYEE as t
   join ( select max(franchise_employee_sk) as MAX_SK from xdm.dim_franchise_employee) as sk
     on 1 = 1 
   where t.insert_batch_sk <> -1

end try
begin catch

   set @MSG = 'INSERT: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch


exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out
if @RET_VAL = 1 return

drop table #TMP_DIM_FRANCHISE_EMPLOYEE

return

/*
create view xdm.DIM_FRANCHISE_EMPLOYEE_CUR with schemabinding as 
select FRANCHISE_EMPLOYEE_SK,
	   PERSON_SK,
	   SCHOOL_SK,
	   FMS_EMPLOYEE_ID,
	   JOB_TITLE,
	   ROW_EFFECTIVE_DATE,
	   ROW_EXPIRATION_DATE,
	   LAST_UPDATE_USER,
	   SRC_SYSTEM_CODE,
	   INSERT_BATCH_SK,
	   UPDATE_BATCH_SK
from xdm.dim_franchise_employee where row_expiration_date = '2999-12-31'

-- drop view xdm.dim_franchise_employee_cur
-- select * from xdm.dim_franchise_employee_cur
*/


 -- select * from #TMP_DIM_FRANCHISE_EMPLOYEE
 -- select * from xdm.dim_franchise_employee where fms_employee_id = 10104934342
 -- select * from xdm.dim_person order by 2
-- select * from xdm.dim_batch_audit order by 1 desc
-- delete from xdm.dim_batch_audit where batch_sk = 11

