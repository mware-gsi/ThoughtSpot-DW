
---------------------------------------------------------------------------------------------------
--
-- Filename: load_dim_incident_type.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-04
--
--  Description:  Loads the DIM_INCIDENT_TYPE table and checks for updates of the source table
--
---------------------------------------------------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_INCIDENT_TYPE]
( 
	[INCIDENT_TYPE_SK]   bigint  NOT NULL ,
	[INCIDENT_TYPE_DESC] varchar(100) NOT NULL ,
	[INCIDENT_TYPE_SUBTYPE_DESC] varchar(25) NULL ,
	[ROW_EFFECTIVE_DATE] date NOT NULL ,
	[ROW_EXPIRATION_DATE] date  NOT NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NOT NULL ,
	[SRC_SYSTEM_CODE]    varchar(10)  NOT NULL ,
	[INSERT_BATCH_SK]    bigint  NOT NULL ,
	[UPDATE_BATCH_SK]    bigint  NOT NULL 
)
go

ALTER TABLE [XDM].[DIM_INCIDENT_TYPE]
	ADD CONSTRAINT [XPKDIM_INCIDENT_TYPE] PRIMARY KEY  CLUSTERED ([INCIDENT_TYPE_SK] ASC)
go
*/


--- DEFAULT ROW
--  insert into xdm.dim_incident_type values (-1, 'DEFAULT INCIDENT TYPE', 'DEFAULT', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), '', -1, -1 )

--  drop table [XDM].[DIM_INCIDENT_TYPE]
--  delete from [XDM].[DIM_INCIDENT_TYPE] where incident_type_sk <> -1
--  select * from xdm.dim_incident_type



-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK
exec xdm.sp_new_batch 'LOAD: DIM_INCIDENT_TYPE', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--- insert new records ---
begin try

   insert into xdm.dim_incident_type
   select sk.max_sk + row_number() over (order by i.title ) as INCIDENT_TYPE_SK, 
          i.title                                           as INCIDENT_TYPE_DESC,
		  null                                              as INCIDENT_SUBTYPE_DESC,               
          cast( '1900-01-01' as date )                      as ROW_EFFECTIVE_DATE,
          cast( '2999-12-31' as date )                      as ROW_EXPIRATION_DATE,
          replace(system_user, 'GODDARDSYSTEMS\', '')       as LAST_UPDATE_USER,
          'CRM'                                             as SRC_SYSTEM_CODE,
          @BATCH_SK                                         as INSERT_BATCH_SK,
	      -1                                                as UPDATE_BATCH_SK
   from ( select title from crm15sql.goddardsystems_mscrm.dbo.subjectbase group by title) as i
   join ( select max(incident_type_sk) as MAX_SK from xdm.dim_incident_type ) as sk
     on 1 = 1
   where not exists( select 1 from xdm.dim_incident_type as es where es.incident_type_desc = i.title collate SQL_Latin1_General_CP1_CI_AS )
        
   set @INS_COUNT = @@ROWCOUNT

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out

end catch


-- select * from xdm.dim_incident_type
-- select * from xdm.dim_batch_audit order by 1 desc


