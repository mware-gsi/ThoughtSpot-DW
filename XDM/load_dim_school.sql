
----------------------------------------------------------
--
-- Filename: load_dim_school.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-01-24
--
--  Description:  Loads the DIM_SCHOOL table and checks for updates of the source table
--
----------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_SCHOOL] 
(	[SCHOOL_SK]         bigint  NOT NULL ,
	[SCHOOL_NUMBER]     integer  NOT NULL ,
	[SCHOOL_NAME]       varchar(50)  NOT NULL ,
	[FMS_SCHOOL_ID]     bigint NOT NULL,
	[SCHOOL_MARKET_SK]  bigint  NOT NULL ,
	[OPENING_DATE]      date  NULL ,
	[CAPACITY]          decimal(6,2)  NOT NULL ,
  	[STATE_CODE]        char(2)  NOT NULL ,
	[ZIP_CODE]          char(5)  NOT NULL ,
	[ROW_EFFECTIVE_DATE]  datetime  NOT NULL ,
	[ROW_EXPIRATION_DATE] datetime  NOT NULL ,
	[LAST_UPDATE_USER]  varchar(25) NOT NULL,
	[SRC_SYSTEM_CODE]   varchar(10)  NOT NULL ,
	[INSERT_BATCH_SK]   bigint  NOT NULL,
	[UPDATE_BATCH_SK]   bigint NOT NULL 
CONSTRAINT [PK_DIM_SCHOOL] PRIMARY KEY CLUSTERED 
(
	[SCHOOL_SK] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
*/

--  drop table [xdm].[DIM_SCHOOL]
--  truncate table [xdm].[DIM_SCHOOL]
--  delete [xdm].[DIM_SCHOOL] where school_sk <> -1
--  select * from [xdm].[DIM_SCHOOL]

--  insert into xdm.dim_school values (-1, -1, 'DEFAULT_SCHOOL', -1, -1, null, 0.0, '', '', '1900-01-01', '2999-12-31','', '', -1, -1 )


-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer = 0
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK ---
exec xdm.sp_new_batch 'LOAD: DIM_SCHOOL', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--------------------------------------
--- load a temporary staging table ---
--------------------------------------

begin try

   select cast(s.number as integer)                   as SCHOOL_NUMBER, 
	      s.name                                      as SCHOOL_NAME, 
		  s.id                                        as FMS_SCHOOL_ID,
	      dsm.school_market_sk                        as SCHOOL_MARKET_SK, 
	      cast(s.opened as date)                      as OPENING_DATE,
		  coalesce( st.ftecapacity, 0.0 )             as CAPACITY,
		  s.region                                    as STATE_CODE,
	      s.postalcode                                as ZIP_CODE,
	      cast('1900-01-01' as date)                  as ROW_EFFECTIVE_DATE,
	      cast('2999-12-31' as date)                  as ROW_EXPIRATION_DATE,
	      replace(system_user, 'GODDARDSYSTEMS\', '') as LAST_UPDATE_USER,
	      'FMS'                                       as SRC_SYSTEM_CODE,
	      -1                                          as INSERT_BATCH_SK,
		  -1                                          as UPDATE_BATCH_SK
   into #TMP_DIM_SCHOOL
   from franchisemanagement.dbo.school as s
   join poc.dim_school_market as dsm
     on s.regionnumber = dsm.school_market_name
--   left outer join franchisemanagement.dbo.statistic as st
--     on s.id = st.schoolid
--    and cast (st.calculationdate as date) = '2019-12-28'
   left outer join ( select schoolid, calculationdate, ftecapacity, row_number() over (partition by schoolid order by calculationdate desc) as CALC_RANK from franchisemanagement.dbo.statistic ) as st
     on s.id = st.schoolid
    and st.calc_rank = 1
   where s.istestschool = 0
--     and s.number < 125

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

   update #TMP_DIM_SCHOOL
   set insert_batch_sk = @BATCH_SK
   where not exists ( select 1 from xdm.dim_school as ds where ds.fms_school_id =  #TMP_DIM_SCHOOL.fms_school_id )

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

   update #TMP_DIM_SCHOOL
   set row_effective_date = getdate(),
       insert_batch_sk = @BATCH_SK
   from #TMP_DIM_SCHOOL as tds
   join xdm.dim_school as ds
     on tds.fms_school_id = ds.fms_school_id
	and ds.row_expiration_date = '2999-12-31'
   where tds.school_number    <> ds.school_number
	  or tds.school_name      <> ds.school_name
	  or tds.school_market_sk <> ds.school_market_sk
	  or tds.opening_date     <> ds.opening_date
      or tds.capacity         <> ds.capacity
	  or tds.state_code       <> ds.state_code
	  or tds.zip_code         <> ds.zip_code
     
   set @UPD_COUNT = @@ROWCOUNT

   --  update #tmp_dim_school set capacity = 140.00 where fms_school_id = 10178407517
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

  update xdm.dim_school
  set xdm.dim_school.row_expiration_date = cast( dateadd(day, -1,  getdate()) as date ),
      xdm.dim_school.update_batch_sk = @batch_sk
  from xdm.dim_school as ds
  join #TMP_DIM_SCHOOL as tds
    on ds.fms_school_id = tds.fms_school_id
   and ds.row_expiration_date = '2999-12-31'
   and tds.insert_batch_sk <> -1
      
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

  update xdm.dim_school
  set xdm.dim_school.row_expiration_date = cast( dateadd(day, -1,  getdate()) as date ),
      xdm.dim_school.update_batch_sk = @batch_sk
  from xdm.dim_school as ds
  where not exists ( select 1 from #TMP_DIM_SCHOOL as tds where ds.fms_school_id = tds.fms_school_id )
    and school_sk <> -1
       
   set @DEL_COUNT = @@ROWCOUNT

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = 'DELETE: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch
--  delete from #tmp_dim_school where fms_school_id = 10102665395


----------------------------------------------------------------------------
--- Insert new/updated records from staging table into the DIM (INSERT)  ---
----------------------------------------------------------------------------

begin try  

   insert into xdm.dim_school 
   select sk.max_sk + row_number() over (order by t.fms_school_id) as SCHOOL_SK, t.* 
   from #TMP_DIM_SCHOOL as t
   join ( select max(school_sk) as MAX_SK from xdm.dim_school) as sk
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

drop table #TMP_DIM_SCHOOL

return

/*
create view xdm.DIM_SCHOOL_CUR with schemabinding as 
select SCHOOL_SK, 
       SCHOOL_NUMBER,
	   SCHOOL_NAME,
	   FMS_SCHOOL_ID,
	   SCHOOL_MARKET_SK,
	   OPENING_DATE,
	   CAPACITY,
	   STATE_CODE,
	   ZIP_CODE,
	   ROW_EFFECTIVE_DATE,
	   ROW_EXPIRATION_DATE,
	   LAST_UPDATE_USER,
	   SRC_SYSTEM_CODE,
	   INSERT_BATCH_SK,
	   UPDATE_BATCH_SK 
from xdm.dim_school where row_expiration_date = '2999-12-31'

-- drop view xdm.dim_school_cur
-- select * from xdm.dim_school_cur
*/


 -- select * from #TMP_DIM_SCHOOL
 -- select * from xdm.dim_school
 -- select * from xdm.dim_school order by 2
-- select * from xdm.dim_batch_audit order by 1 desc
-- delete from xdm.dim_batch_audit where batch_sk = 11

