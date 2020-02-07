
---------------------------------------------------------------------------------------
--
-- Filename: load_dim_child.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-05
--
--  Description:  Loads the DIM_CHILD table and checks for updates of the source table
--
---------------------------------------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_CHILD]
( 
	[CHILD_SK]           bigint  NOT NULL ,
	[PERSON_SK]          bigint  NULL ,
	[SCHOOL_SK]          bigint  NULL ,
	[FMS_CHILD_ID]       bigint  NULL ,
	[DROP_IN_IND]        BIT  NULL ,
	[ROW_EFFECTIVE_DATE] date  NULL ,
	[ROW_EXPIRATION_DATE] date  NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NULL ,
	[SRC_SYSTEM_CODE]    varchar(10)  NULL ,
	[INSERT_BATCH_SK]    bigint  NULL ,
	[UPDATE_BATCH_SK]    bigint  NULL 
)
go

ALTER TABLE [XDM].[DIM_CHILD]
	ADD CONSTRAINT [XPKDIM_CHILD] PRIMARY KEY  CLUSTERED ([CHILD_SK] ASC)
go
*/

--  drop table [XDM].[DIM_CHILD]
--  truncate table [xdm].[DIM_CHILD]
--  delete [xdm].[DIM_CHILD] where person_sk <> -1
--  select * from [xdm].[DIM_CHILD]

--  insert into xdm.dim_child values (-1, -1, -1, -1, 0,  '1900-01-01', '2999-12-31','', '', -1, -1 )


-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer = 0
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK ---
exec xdm.sp_new_batch 'LOAD: DIM_CHILD', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--------------------------------------
--- load a temporary staging table ---
--------------------------------------

begin try

   select p.person_sk,
	      ds.school_sk,
	      c.id                                        as FMS_CHILD_ID,
	      c.isdropinonly                              as DROP_IN_IND,
	      --cast( p.birthdate as date )                 as DATE_OF_BIRTH,
  	      cast('1900-01-01' as date)                  as ROW_EFFECTIVE_DATE,
	      cast('2999-12-31' as date)                  as ROW_EXPIRATION_DATE,
	      replace(system_user, 'GODDARDSYSTEMS\', '') as LAST_UPDATE_USER,
	      'FMS'                                       as SRC_SYSTEM_CODE,
	      -1                                          as INSERT_BATCH_SK,
		  -1                                          as UPDATE_BATCH_SK
   into #TMP_DIM_CHILD
   from franchisemanagement.dbo.child as c
   join poc.dim_person as p
     on c.id = p.fms_person_id
   join franchisemanagement.dbo.school as s
     on c.schoolid = s.id
   join poc.dim_school as ds
     on cast( s.number as integer ) = cast( ds.school_number as integer )

   --  select * from #TMP_DIM_CHILD
   --  drop table #TMP_DIM_CHILD

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

   update #TMP_DIM_CHILD
   set insert_batch_sk = @BATCH_SK
   where not exists ( select 1 from xdm.dim_child as ds where ds.fms_child_id =  #TMP_DIM_CHILD.fms_child_id )

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

   update #TMP_DIM_CHILD
   set row_effective_date = cast( getdate() as date),
       insert_batch_sk = @BATCH_SK
   from #TMP_DIM_CHILD as tdc
   join xdm.dim_child as dc
     on tdc.fms_child_id = dc.fms_child_id
	and dc.row_expiration_date = '2999-12-31'
   where tdc.person_sk   <> dc.person_sk
	  or tdc.school_sk   <> dc.school_sk
	  or tdc.drop_in_ind <> dc.drop_in_ind

     
   set @UPD_COUNT = @@ROWCOUNT

   --  update #tmp_dim_child set drop_in_ind = 1 where fms_child_id = 10100177063
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

  update xdm.dim_child
  set xdm.dim_child.row_expiration_date = cast( dateadd(day, -1,  getdate()) as date ),
      xdm.dim_child.update_batch_sk = @BATCH_SK
  from xdm.dim_child as dc
  join #TMP_DIM_CHILD as tdc
    on dc.fms_child_id = tdc.fms_child_id
   and dc.row_expiration_date = '2999-12-31'
   and tdc.insert_batch_sk <> -1
      
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

  update xdm.dim_child
  set xdm.dim_child.row_expiration_date = cast( dateadd(day, -1,  getdate()) as date ),
      xdm.dim_child.update_batch_sk = @BATCH_SK
  from xdm.dim_child as dc
  where not exists ( select 1 from #TMP_DIM_CHILD as tdc where dc.fms_child_id = tdc.fms_child_id )
    and dc.person_sk <> -1
       
   set @DEL_COUNT = @@ROWCOUNT

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = 'DELETE: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch
--  delete from #tmp_dim_child where fms_child_id = 10104944085


----------------------------------------------------------------------------
--- Insert new/updated records from staging table into the DIM (INSERT)  ---
----------------------------------------------------------------------------

begin try  

   insert into xdm.dim_child
   select sk.max_sk + row_number() over (order by t.fms_child_id) as PERSON_SK, t.* 
   from #TMP_DIM_CHILD as t
   join ( select max(child_sk) as MAX_SK from xdm.dim_child) as sk
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

drop table #TMP_DIM_CHILD

return

/*
create view xdm.DIM_CHILD_CUR with schemabinding as 
select CHILD_SK,
	   PERSON_SK,
	   SCHOOL_SK,
	   FMS_CHILD_ID,
       DROP_IN_IND,
       ROW_EFFECTIVE_DATE,
	   ROW_EXPIRATION_DATE,
	   LAST_UPDATE_USER,
	   SRC_SYSTEM_CODE,
	   INSERT_BATCH_SK,
	   UPDATE_BATCH_SK
from xdm.dim_child where row_expiration_date = '2999-12-31'

-- drop view xdm.dim_child_cur
-- select * from xdm.dim_child_cur
*/


 -- select * from #TMP_DIM_CHILD
 -- select * from xdm.dim_child where fms_child_id = 10100177063
 -- select * from xdm.dim_person order by 2
-- select * from xdm.dim_batch_audit order by 1 desc
-- delete from xdm.dim_batch_audit where batch_sk = 11

