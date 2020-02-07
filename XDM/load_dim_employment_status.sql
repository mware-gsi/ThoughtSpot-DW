
---------------------------------------------------------------------------------------------------
--
-- Filename: load_dim_employment_status.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-04
--
--  Description:  Loads the DIM_EMPLOYMENT_STATUS table and checks for updates of the source table
--
---------------------------------------------------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_EMPLOYMENT_STATUS]
( 
	[EMPLOYMENT_STATUS_SK] bigint  NOT NULL ,
	[EMPLOYMENT_STATUS_DESC] varchar(50)  NOT NULL ,
	[ROW_EFFECTIVE_DATE] date  NOT NULL ,
	[ROW_EXPIRATION_DATE] date  NOT NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NOT NULL ,
	[SRC_SYSTEM_CODE]    varchar(15) NOT  NULL ,
	[INSERT_BATCH_SK]    bigint  NOT NULL ,
	[UPDATE_BATCH_SK]    bigint  NOT NULL 
)
go

ALTER TABLE [XDM].[DIM_EMPLOYMENT_STATUS]
	ADD CONSTRAINT [XPKDIM_EMPLOYMENT_STATUS] PRIMARY KEY  CLUSTERED ([EMPLOYMENT_STATUS_SK] ASC)
go
*/


--- DEFAULT ROW
--  insert into xdm.dim_employment_status values (-1, 'DEFAULT EMPLOYMENT STATUS', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), '', -1, -1 )

--  drop table [XDM].[DIM_EMPLOYMENT_STATUS]
--  delete from [XDM].[DIM_EMPLOYMENT_STATUS] where employment_status_sk <> -1
--  select * from xdm.dim_employment_status



-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK
exec xdm.sp_new_batch 'LOAD: DIM_EMPLOYMENT_STATUS', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--- insert new records ---
begin try

   insert into xdm.dim_employment_status
   select sk.max_sk + row_number() over (order by ee.[type] ) as EMPLOYMENT_STATUS_SK, 
          ee.[type]                                           as EMPLOYMENT_STATUS_DESC,
          cast( '1900-01-01' as date )                        as ROW_EFFECTIVE_DATE,
          cast( '2999-12-31' as date )                        as ROW_EXPIRATION_DATE,
          replace(system_user, 'GODDARDSYSTEMS\', '')         as LAST_UPDATE_USER,
          'FMS'                                               as SRC_SYSTEM_CODE,
          @BATCH_SK                                           as INSERT_BATCH_SK,
	      -1                                                  as UPDATE_BATCH_SK
   from ( select [type] from franchisemanagement.dbo.employeeevent group by [type] )as ee
   join ( select max(employment_status_sk) as MAX_SK from xdm.dim_employment_status ) as sk
     on 1 = 1
   where not exists( select 1 from xdm.dim_employment_status as es where es.employment_status_desc = ee.[type] )
        
   set @INS_COUNT = @@ROWCOUNT

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out

end catch


-- select * from xdm.dim_employment_status
-- select * from xdm.dim_batch_audit order by 1 desc


