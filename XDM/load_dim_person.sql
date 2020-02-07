
---------------------------------------------------------------------------------------
--
-- Filename: load_dim_person.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-04
--
--  Description:  Loads the DIM_PERSON table and checks for updates of the source table
--
---------------------------------------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_PERSON]
( 
	[PERSON_SK]          bigint  NOT NULL ,
	[FMS_PERSON_ID]      BIGINT  NOT NULL ,
	[FIRST_NAME]         varchar(100)  NULL ,
	[LAST_NAME]          varchar(100)  NULL ,
	[DATE_OF_BIRTH]      datetime  NULL ,
	[ROW_EFFECTIVE_DATE] date  NOT NULL ,
	[ROW_EXPIRATION_DATE] date  NOT NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NOT NULL ,
	[SRC_SYSTEM_CODE]    varchar(10)  NOT NULL ,
	[INSERT_BATCH_SK]    bigint  NOT NULL ,
	[UPDATE_BATCH_SK]    bigint  NOT NULL 
)
go

ALTER TABLE [XDM].[DIM_PERSON]
	ADD CONSTRAINT [XPKDIM_PERSON] PRIMARY KEY  CLUSTERED ([PERSON_SK] ASC)
go
*/

--  drop table [xdm].[DIM_PERSON]
--  truncate table [xdm].[DIM_PERSON]
--  delete [xdm].[DIM_PERSON] where person_sk <> -1
--  select * from [xdm].[DIM_PERSON]

--  insert into xdm.dim_person values (-1, -1, 'DEFAULT PERSON', 'DEFAULT PERSON', null,  '1900-01-01', '2999-12-31','', '', -1, -1 )


-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer = 0
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK ---
exec xdm.sp_new_batch 'LOAD: DIM_PERSON', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--------------------------------------
--- load a temporary staging table ---
--------------------------------------

begin try

   select p.id                                        as FMS_PERSON_ID,
          p.givenname                                 as FIRST_NAME,
	      p.familyname                                as LAST_NAME,
	      cast( p.birthdate as date )                 as DATE_OF_BIRTH,
  	      cast('1900-01-01' as date)                  as ROW_EFFECTIVE_DATE,
	      cast('2999-12-31' as date)                  as ROW_EXPIRATION_DATE,
	      replace(system_user, 'GODDARDSYSTEMS\', '') as LAST_UPDATE_USER,
	      'FMS'                                       as SRC_SYSTEM_CODE,
	      -1                                          as INSERT_BATCH_SK,
		  -1                                          as UPDATE_BATCH_SK
   into #TMP_DIM_PERSON
   from franchisemanagement.dbo.person as p
   --where id >= 42706525488

   --  select * from #TMP_DIM_PERSON
   --  drop table #TMP_DIM_PERSON

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

   update #TMP_DIM_PERSON
   set insert_batch_sk = @BATCH_SK
   where not exists ( select 1 from xdm.dim_person as ds where ds.fms_person_id =  #TMP_DIM_PERSON.fms_person_id )

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

   update #TMP_DIM_PERSON
   set row_effective_date = cast( getdate() as date),
       insert_batch_sk = @BATCH_SK
   from #TMP_DIM_PERSON as tdp
   join xdm.dim_person as dp
     on tdp.fms_person_id = dp.fms_person_id
	and dp.row_expiration_date = '2999-12-31'
   where tdp.first_name    <> dp.first_name
	  or tdp.last_name      <> dp.last_name
	  or tdp.date_of_birth <> dp.date_of_birth

     
   set @UPD_COUNT = @@ROWCOUNT

   --  update #tmp_dim_person set last_name = 'Mentery' where fms_person_id = 42739907905
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

  update xdm.dim_person
  set xdm.dim_person.row_expiration_date = cast( dateadd(day, -1,  getdate()) as date ),
      xdm.dim_person.update_batch_sk = @batch_sk
  from xdm.dim_person as dp
  join #TMP_DIM_PERSON as tdp
    on dp.fms_person_id = tdp.fms_person_id
   and dp.row_expiration_date = '2999-12-31'
   and tdp.insert_batch_sk <> -1
      
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

  update xdm.dim_person
  set xdm.dim_person.row_expiration_date = cast( dateadd(day, -1,  getdate()) as date ),
      xdm.dim_person.update_batch_sk = @batch_sk
  from xdm.dim_person as dp
  where not exists ( select 1 from #TMP_DIM_PERSON as tdp where dp.fms_person_id = tdp.fms_person_id )
    and dp.person_sk <> -1
       
   set @DEL_COUNT = @@ROWCOUNT

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = 'DELETE: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch
--  delete from #tmp_dim_person where fms_person_id = 42739907908


----------------------------------------------------------------------------
--- Insert new/updated records from staging table into the DIM (INSERT)  ---
----------------------------------------------------------------------------

begin try  

   insert into xdm.dim_person
   select sk.max_sk + row_number() over (order by t.fms_person_id) as PERSON_SK, t.* 
   from #TMP_DIM_PERSON as t
   join ( select max(person_sk) as MAX_SK from xdm.dim_person) as sk
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

drop table #TMP_DIM_PERSON

return

/*
create view xdm.DIM_PERSON_CUR with schemabinding as 
select PERSON_SK,
	   FMS_PERSON_ID,
	   FIRST_NAME,
	   LAST_NAME,
	   DATE_OF_BIRTH,
	   ROW_EFFECTIVE_DATE,
	   ROW_EXPIRATION_DATE,
	   LAST_UPDATE_USER,
	   SRC_SYSTEM_CODE,
 	   INSERT_BATCH_SK,
	   UPDATE_BATCH_SK 
from xdm.dim_person where row_expiration_date = '2999-12-31'

-- drop view xdm.dim_person_cur
-- select * from xdm.dim_person_cur
*/


 -- select * from #TMP_DIM_PERSON
 -- select * from xdm.dim_person 
 -- select * from xdm.dim_person order by 2
-- select * from xdm.dim_batch_audit order by 1 desc
-- delete from xdm.dim_batch_audit where batch_sk = 11

