
---------------------------------------------------------------------------------------------
--
-- Filename: load_dim_site_visit_type.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-03
--
--  Description:  Loads the DIM_SITE_VISIT table and checks for updates of the source table
--
--------------------------------------------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_SITE_VISIT_TYPE]
( 
	[SITE_VISIT_TYPE_SK] bigint  NOT NULL ,
	[SITE_VISIT_TYPE_NAME] varchar(50)  NULL ,
	[ROW_EFFECTIVE_DATE] date  NULL ,
	[ROW_EXPIRATION_DATE] date  NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NULL ,
	[SRC_SYSTEM_CODE]    varchar(15)  NULL ,
	[INSERT_BATCH_SK]    bigint  NULL ,
	[UPDATE_BATCH_SK]    bigint  NULL 
)
go

ALTER TABLE [XDM].[DIM_SITE_VISIT_TYPE]
	ADD CONSTRAINT [XPKDIM_SITE_VISIT_TYPE] PRIMARY KEY  CLUSTERED ([SITE_VISIT_TYPE_SK] ASC)
go
*/


--- DEFAULT ROW
--  insert into xdm.dim_site_visit_type values (-1, 'DEFAULT SITE VISIT TYPE', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), '', -1, -1 )

--  drop table [XDM].[DIM_SITE_VISIT_TYPE]
--  delete from [XDM].[DIM_SITE_VISIT_TYPE] where school_market_sk <> -1
--  select * from xdm.dim_site_visit_type



-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK
exec xdm.sp_new_batch 'LOAD: DIM_SITE_VISIT', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--- insert new records ---
begin try

   insert into xdm.dim_site_visit_type
   select sk.max_sk + row_number() over (order by f.form_id) as SITE_VISIT_TYPE_SK, 
          f.form_name,
          cast( '1900-01-01' as date )                       as ROW_EFFECTIVE_DATE,
          cast( '2999-12-31' as date )                       as ROW_EXPIRATION_DATE,
          replace(system_user, 'GODDARDSYSTEMS\', '')        as LAST_UPDATE_USER,
          'FORMAPP'                                          as SRC_SYSTEM_CODE,
          @BATCH_SK                                          as INSERT_BATCH_SK,
	      -1                                                 as UPDATE_BATCH_SK
   from [websqlprod].formsappnew.forms.forms as f
   join ( select max(site_visit_type_sk) as MAX_SK from xdm.dim_site_visit_type) as sk
     on 1 = 1
   where not exists( select 1 from xdm.dim_site_visit_type as dsv where dsv.site_visit_type_name = f.form_name )
        
   set @INS_COUNT = @@ROWCOUNT

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out

end catch


-- select * from xdm.dim_site_visit_type
-- select * from xdm.dim_batch_audit order by 1 desc


