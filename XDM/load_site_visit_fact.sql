
----------------------------------------------------------
--
-- Filename: load_site_visit_fact.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-06
--
--  Description:  Loads the SITE_VISIT_FACT table 
--
----------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [xdm].[SITE_VISIT_FACT](
	[SCHOOL_SK] bigint NOT NULL,
    [SITE_VISIT_TYPE_SK] bigint not null,
	[SITE_VISIT_DATE_SK] bigint not null,
	[PASS_IND] bit not null,
	[SCORE_VALUE] decimal(10,4)
 CONSTRAINT [PK_SITE_VISIT_FACT] PRIMARY KEY CLUSTERED 
(
	[SCHOOL_SK] ASC, [SITE_VISIT_TYPE_SK], [SITE_VISIT_DATE_SK]
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
*/

--  drop table [xdm].[SITE_VISIT_FACT]
--  select * from [xdm].[SITE_VISIT_FACT]

-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer = 0
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


--- clear table ---
delete [xdm].[SITE_VISIT_FACT]
set @DEL_COUNT = @@ROWCOUNT

-- Get new Batch SK ---
exec xdm.sp_new_batch 'LOAD: SITE_VISIT_FACT', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


----------------------------------------------------------------------
--- set BATCH_SK for records in temp table not found in DIM (NEW)  ---
----------------------------------------------------------------------

begin try

   insert into xdm.site_visit_fact
   select --f.form_name, s.school_number, cast( r.time_stamp as date ) REVIEW_DATE, 
          ds.school_sk, 
	      dsvt.site_visit_type_sk, 
	      dd.date_sk, 
	      case when r.results = 'Passed' then 1
	           else 0
	      end as SITE_VISIT_PASS_IND,
	      r.finalscore
   from [az-gsisql].[2018qa].dbo.review as r
   join [websqlprod].formsappnew.forms.forms as f
     on r.form_id = f.form_id
   join [az-gsisql].[2018qa].dbo.schools as s
     on r.school_id = s.school_id
   join xdm.dim_school as ds
     on cast( s.school_number as integer ) = cast( ds.school_number as integer)
   join xdm.dim_site_visit_type as dsvt
     on f.form_name = dsvt.site_visit_type_name
   join xdm.dim_date as dd
     on cast( r.time_stamp as date ) = dd.date_value
   where r.submitted = 1
     and r.form_id = 1
     and r.finalscore is not null
     and cast(r.time_stamp as date) between '2016-01-01' and cast( getdate() as date) 
   --order by 1,2,3

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

-- select * from xdm.enrollment_fact
-- select * from xdm.dim_batch_audit order by 1 desc


