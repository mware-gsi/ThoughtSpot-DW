
----------------------------------------------------------
--
-- Filename: load_employment_fact.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-06
--
--  Description:  Loads the EMPLOYMENT_FACT table 
--
----------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [xdm].[FRANCHISE_EMPLOYMENT_FACT](
	[FRANCHISE_EMPLOYEE_SK] bigint NOT NULL,
    [EMPLOYMENT_STATUS_SK] bigint not null,
	[EMPLOYMENT_STATUS_DATE_SK] bigint not null
 CONSTRAINT [PK_FRANCHISE_EMPLOYMENT_FACT] PRIMARY KEY CLUSTERED 
(
	[FRANCHISE_EMPLOYEE_SK] ASC, [EMPLOYMENT_STATUS_SK], [EMPLOYMENT_STATUS_DATE_SK]
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
*/

--  drop table [xdm].[FRANCHISE_EMPLOYMENT_FACT]
--  select * from [xdm].[FRANCHISE_EMPLOYMENT_FACT]

-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer = 0
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


--- clear table ---
delete [xdm].[FRANCHISE_EMPLOYMENT_FACT]
set @DEL_COUNT = @@ROWCOUNT


-- Get new Batch SK ---
exec xdm.sp_new_batch 'LOAD: FRANCHISE_EMPLOYMENT_FACT', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


----------------------------------------------------------------------
--- set BATCH_SK for records in temp table not found in DIM (NEW)  ---
----------------------------------------------------------------------

begin try

   insert into xdm.franchise_employment_fact
   select dfe.franchise_employee_sk, 
          [des].employment_status_sk, 
	      dd.date_sk
   from ( select strt.employeeid, strt.[type], strt.dateofoccurrence as START_DATE, 
                 case when nd.dateofoccurrence is null and strt.[type]  = 'SEPARATION' then strt.dateofoccurrence
                      when nd.dateofoccurrence is null and strt.[type] in ('HIRE', 'ACTIVE') then cast( getdate() as date)
                      else nd.dateofoccurrence 
                 end as END_DATE
          from ( select ee.employeeid, ee.[type], ee.dateofoccurrence, row_number() over ( partition by ee.employeeid order by ee.dateofoccurrence, ee.type desc ) as EMP_RANK
                 from franchisemanagement.dbo.employeeevent as ee
                 where dateofoccurrence between '1988-01-01' and cast( getdate() as date )
                 --and ee.employeeid <= 19204431965
	           ) as strt
          left join ( select ee.employeeid, ee.[type], ee.dateofoccurrence, row_number() over ( partition by ee.employeeid order by ee.dateofoccurrence, ee.type desc ) as EMP_RANK
                      from franchisemanagement.dbo.employeeevent as ee
                      where dateofoccurrence between '1988-01-01' and cast( getdate() as date)
                      --and ee.employeeid <= 10612658606
         	        ) as nd
            on strt.employeeid   = nd.employeeid
		   and strt.emp_rank + 1 = nd.emp_rank
        ) as x
   join xdm.dim_franchise_employee as dfe
     on x.employeeid = dfe.fms_employee_id
   join xdm.dim_employment_status as [des]
     on x.[type] = [des].employment_status_desc
   join xdm.dim_date as dd
     on dd.date_value between x.[start_date] and x.end_date
   where date_sk between 20180101 and 20191231
   group by dfe.franchise_employee_sk, [des].employment_status_sk, dd.date_sk
   order by 1, 3

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


