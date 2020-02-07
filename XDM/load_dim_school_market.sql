
----------------------------------------------------------
--
-- Filename: load_dim_school_market.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-01-23
--
--  Description:  Loads the DIM_SCHOOL_MARKET table and checks for updates of the source table
--
----------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_SCHOOL_MARKET]
( 
	[SCHOOL_MARKET_SK]    bigint  NOT NULL ,
	[SCHOOL_MARKET_NAME]  VARCHAR (100)  NULL ,
	[ROW_EFFECTIVE_DATE]  DATETIME  NULL ,
	[ROW_EXPIRATION_DATE] DATETIME  NULL ,
	[LAST_UPDATE_USER]    VARCHAR(25)  NULL ,
	[SRC_SYSTEM_CODE]     VARCHAR(10)  NULL ,
	[INSERT_BATCH_SK]     BIGINT  NULL,
	[UPDATE_BATCH_SK]     bigint NOT NULL 
)

ALTER TABLE [XDM].[DIM_SCHOOL_MARKET]
	ADD CONSTRAINT [XPKDIM_SCHOOL_MARKET] PRIMARY KEY  CLUSTERED ([SCHOOL_MARKET_SK] ASC)
*/


--- DEFAULT ROW
--  insert into xdm.dim_school_market values (-1, 'DEFAULT SCHOOL MARKET', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), '', -1, -1 )

--  drop table [XDM].[DIM_SCHOOL_MARKET]
--  delete from [XDM].[DIM_SCHOOL_MARKET] where school_market_sk <> -1



-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK
exec xdm.sp_new_batch 'LOAD: DIM_SCHOOL_MARKET', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--- insert new records ---
begin try

   insert into xdm.dim_school_market
   select sk.max_sk + row_number() over (order by regionnumber) as SCHOOL_MARKET_SK,
          s.regionnumber                              as SCHOOL_MARKET_NAME,
          getdate()                                   as ROW_EFFECTIVE_DATE,
	      getdate()                                   as ROW_EXPIRATION_DATE,
	      replace(system_user, 'GODDARDSYSTEMS\', '') as LAST_UPDATE_USER,
	      'FMS'                                       as SRC_SYSTEM_CODE,
	      @BATCH_SK                                   as INSERT_BATCH_SK,
		  -1                                          as UPDATE_BATCH_SK
   from ( select regionnumber from franchisemanagement.dbo.school where istestschool = 0 group by regionnumber ) as s
   join ( select max(school_market_sk) as MAX_SK from xdm.dim_school_market ) as sk
     on 1 = 1
   where not exists ( select 1 from xdm.dim_school_market as dsm where dsm.SCHOOL_MARKET_NAME = s.regionnumber )
     
   set @INS_COUNT = @@ROWCOUNT

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out

end catch


-- select * from xdm.dim_school_market
-- select * from xdm.dim_batch_audit order by 1 desc


