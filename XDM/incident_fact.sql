
----------------------------------------------------------
--
-- Filename: load_incident_fact.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-06
--
--  Description:  Loads the INCIDENT_FACT table 
--
----------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[INCIDENT_FACT](
	[SCHOOL_SK] bigint NOT NULL,
    --[CHILD_SK] bigint not null,
	[INCIDENT_TYPE_SK] bigint not null,
	[INCIDENT_DATE] datetime not null,
	[INCIDENT_DESC] varchar(max)
 CONSTRAINT [PK_INCIDENT_FACT] PRIMARY KEY CLUSTERED 
(
	[SCHOOL_SK] ASC, [INCIDENT_TYPE_SK], [INCIDENT_DATE]
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
*/

--  drop table [xdm].[INCIDENT_FACT]
--  select * from [xdm].[INCIDENT_FACT]

-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer = 0
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


--- clear table ---
delete [xdm].[INCIDENT_FACT]
set @DEL_COUNT = @@ROWCOUNT

-- Get new Batch SK ---
exec xdm.sp_new_batch 'LOAD: INCIDENT_FACT', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


----------------------------------------------------------------------
--- set BATCH_SK for records in temp table not found in DIM (NEW)  ---
----------------------------------------------------------------------

begin try

   insert into xdm.incident_fact

   --select school_sk, incident_type_sk, INCIDENT_DATE_TIME, count(1) from (

   select ds.school_sk, 
          it.incident_type_sk, 
	      cast(i.createdon as datetime ) as INCIDENT_DATE_TIME, 
		  i.[description]
   from goddardsystems_mscrm.dbo.incidentbase as i
   join goddardsystems_mscrm.dbo.accountbase as a
     on i.customerid = a.accountid
   join goddardsystems_mscrm.dbo.subjectbase as s
     on i.subjectid = s.subjectid
   join xdm.dim_school as ds
     on cast( a.accountnumber as integer ) = cast( ds.school_number as integer ) 
   join xdm.dim_incident_type as it
     on s.title = it.incident_type_desc collate SQL_Latin1_General_CP1_CI_AS
   where cast( i.createdon as date ) between '2006-01-01' and cast( getdate() as date )

   --) as x
   --group by school_sk, incident_type_sk, INCIDENT_DATE_TIME
   --order by 4 desc


   set @INS_COUNT = @@ROWCOUNT

end try
begin catch

   set @MSG = 'LOAD: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch


exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, null, @RET_VAL out
if @RET_VAL = 1 return

return

-- select * from xdm.incident_fact
-- select * from xdm.dim_batch_audit order by 1 desc


