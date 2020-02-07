
---------------------------------------------------------------------------------------------------------
--
-- Filename: load_dim_program.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-03
--
--  Description:  Loads the DIM_PROGRAM table and checks for updates of the source table
--                There is no source table for this, lookup values are supplied by the FMS user interface
--                PROGRAM_SK corresponds tp the program code used in FMS
--
---------------------------------------------------------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_PROGRAM]
( 
	[PROGRAM_SK]         bigint  NOT NULL ,
	[PROGRAM_NAME]       varchar(50)  NULL ,
	[ROW_EFFECTIVE_DATE] date  NULL ,
	[ROW_EXPIRATION_DATE] date  NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NULL ,
	[SRC_SYSTEM_CODE]    varchar(15)  NULL ,
	[INSERT_BATCH_SK]    bigint  NULL ,
	[UPDATE_BATCH_SK]    bigint  NULL 
)
go

ALTER TABLE [XDM].[DIM_PROGRAM]
	ADD CONSTRAINT [XPKDIM_PROGRAM] PRIMARY KEY  CLUSTERED ([PROGRAM_SK] ASC)
go
*/


--- DEFAULT ROW
--  insert into xdm.dim_program values (-1, 'DEFAULT PROGRAM', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), '', -1, -1 )

--  drop table [XDM].[DIM_PROGRAM]
--  delete from [XDM].[DIM_PROGRAM] where program_sk <> -1



-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK
exec xdm.sp_new_batch 'LOAD: DIM_PROGRAM', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--- insert new records ---
begin try

insert into xdm.dim_program values (0, 'NONE', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (1, 'Infants', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (2, 'First Stemps', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (3, 'Pre Toddlers', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (4, 'Toddlers', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (5, 'Get Set', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (6, 'Pre School', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (8, 'Pre Kindergarten', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (9, 'Kindergarten', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (10, 'Kindergarten Enrichment', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (11, 'Before & After School', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (12, 'Before School', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (13, 'After School', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (14, 'Summer Program', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (15, 'Toddleroo', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (16, 'Pre School Prep', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (17, 'State Pre K', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (18, 'Junior Kindergarten', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
insert into xdm.dim_program values (19, 'First Grade', cast('1900-01-01' as date), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), 'FMS', @BATCH_SK, -1 )
     
   set @INS_COUNT = 19

   exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out

end try
begin catch

   set @MSG = error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out

end catch


-- select * from xdm.dim_program
-- select * from xdm.dim_batch_audit order by 1 desc


