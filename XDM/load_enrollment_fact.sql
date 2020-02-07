
----------------------------------------------------------
--
-- Filename: load_enrollment_fact.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-05
--
--  Description:  Loads the ENROLLMENT_FACT table 
--
----------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[ENROLLMENT_FACT]
( 
	[SCHOOL_SK]          bigint  NOT NULL ,
	[CHILD_SK]           bigint  NOT NULL ,
	[PROGRAM_SK]         bigint  NOT NULL ,
	[ENROLLMENT_STATUS_DATE_SK] bigint  NOT NULL ,
	[ENROLLMENT_STATUS_SK] bigint  NOT NULL ,
	[ROW_EFFECTIVE_DATE] date  NOT NULL ,
	[ROW_EXPIRATION_DATE] date  NOT NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NOT NULL ,
	[SRC_SYSTEM_CODE]    varchar(10)  NOT NULL ,
	[INSERT_BATCH_SK]    bigint  NOT NULL ,
	[UPDATE_BATCH_SK]    bigint  NOT NULL 
)
go

ALTER TABLE [XDM].[ENROLLMENT_FACT]
	ADD CONSTRAINT [XPKENROLLMENT_FACT] PRIMARY KEY  CLUSTERED ([SCHOOL_SK] ASC,[CHILD_SK] ASC,[PROGRAM_SK] ASC,[ENROLLMENT_STATUS_DATE_SK] ASC,[ENROLLMENT_STATUS_SK] ASC)
go
*/

--  drop table [xdm].[ENROLLMENT_FACT]
--  select * from [xdm].[ENROLLMENT_FACT]


--- clear table ---
truncate table [xdm].[ENROLLMENT_FACT]


-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer = 0
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)


-- Get new Batch SK ---
exec xdm.sp_new_batch 'LOAD: ENROLLMENT_FACT', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


----------------------------------
--- Load enrollment work table ---
----------------------------------

/*
create table xdm.WORK_ENROLL_FACT(
    [childid] bigint not null,
	[type] varchar(25) NOT NULL,
	[dateofoccurrence] date not null,
	[newprimaryprogram] integer not null,
	[id] bigint,
	row_rank integer
 CONSTRAINT [PK_WORK_ENROLL_FACT] PRIMARY KEY CLUSTERED 
(
	[childid] ASC, [type], [dateofoccurrence], [newprimaryprogram], [id]
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
*/

--  drop table xdm.WORK_ENROLL_FACT
--  select top 1000 * from xdm.work_enroll_fact

truncate table xdm.WORK_ENROLL_FACT


begin try

   insert into xdm.work_enroll_fact
   select childid, 
          [type], 
		  dateofoccurrence, 
		  coalesce(newprimaryprogram, -1) as NEW_PRIMARY_PROGRAM, 
		  id, 
		  row_number () over ( partition by childid order by dateofoccurrence, id ) as row_rank 
   from ( select e.id, 
                 e.childid, 
				 e.[type], 
				 cast( e.dateofoccurrence as date) as DATEOFOCCURRENCE, 
				 ee.newprimaryprogram
          from franchisemanagement.dbo.[event] as e
          left join franchisemanagement.dbo.event_enrollment as ee
            on e.id = ee.id
          where e.dateofoccurrence between '2014-01-01' and '2019-12-31'
          --and childid = 12123077000

          union all

          select -1, childid, 'ENROLLMENT', cast(actualtransitiondate as date), nextprimaryprogram
          from ( select childid, actualtransitiondate, nextprimaryprogram, row_number() over (partition by childid, nextprimaryprogram order by actualtransitiondate, version ) as TRANS_RANK 
                 from franchisemanagement.dbo.transition 
	            --where childid = 12123077000 
	           ) as x 
	      where trans_rank = 1
        ) as x
        --group by childid, [type], dateofoccurrence, id, coalesce(newprimaryprogram, -1), id
        order by 1,6

end try
begin catch

   set @MSG = 'WORK: ' + error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch


----------------------------------------------------------------------
--- set BATCH_SK for records in temp table not found in DIM (NEW)  ---
----------------------------------------------------------------------

begin try

   insert into xdm.enrollment_fact
   select --dd.date_value, 
          --x.childid, 
	      --x.[type], 
	      ds.school_sk, 
	      dc.child_sk, 
	      x.newprimaryprogram,
	      dd.date_sk,
	      de.enrollment_status_sk,
  	      cast('1900-01-01' as date)                  as ROW_EFFECTIVE_DATE,
	      cast('2999-12-31' as date)                  as ROW_EXPIRATION_DATE,
	      replace(system_user, 'GODDARDSYSTEMS\', '') as LAST_UPDATE_USER,
	      'FMS'                                       as SRC_SYSTEM_CODE,
	      @BATCH_SK                                   as INSERT_BATCH_SK,
		  -1                                          as UPDATE_BATCH_SK

   from ( select strt.childid, 
                 strt.dateofoccurrence as START_DATE, 

	             case when nd.dateofoccurrence is null and strt.[type] in ('DEACTIVATION', 'GRADUATION') then strt.dateofoccurrence
	                  when nd.dateofoccurrence is null and strt.[type] in ('REGISTRATION', 'ENROLLMENT') then cast( getdate() as date)
			          else nd.dateofoccurrence 
	             end as END_DATE, 

	             strt.[type], 
	             strt.newprimaryprogram 
          from xdm.work_enroll_fact as strt
          left join xdm.work_enroll_fact as nd
            on strt.childid = nd.childid
		   and strt.row_rank + 1 = nd.row_rank 
		  --where strt.childid = 12123077000
		  --order by 3
        ) as x
   join xdm.dim_date as dd
     on dd.date_value between x.[start_date] and x.end_date
   join xdm.dim_child as dc
     on x.childid = dc.fms_child_id
   join franchisemanagement.dbo.child as c
     on x.childid = c.id
   join franchisemanagement.dbo.school as s
     on c.schoolid = s.id
   join xdm.dim_school as ds
     on cast(s.number as integer) = cast(ds.school_number as integer)
   join xdm.dim_enrollment_status as de
     on x.[type] = de.enrollment_status_desc
   where dd.date_value between '2019-01-01' and '2019-12-31'
   group by ds.school_sk, dc.child_sk, x.newprimaryprogram, dd.date_sk, de.enrollment_status_sk

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


