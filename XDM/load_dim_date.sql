
-------------------------------------------------------------------------------------
--
-- Filename: load_dim_date.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-02-03
--
--  Description:  Loads the DIM_DATE table and checks for updates of the source table
--
-------------------------------------------------------------------------------------

use FMS_PROFILE

/*
CREATE TABLE [XDM].[DIM_DATE]
( 
	[DATE_SK]            bigint  NOT NULL ,
	[DATE_VALUE]         date  NOT NULL ,
	[DAY_OF_WEEK]        tinyint  NOT NULL ,
	[DAY_NAME]           varchar(15)  NOT NULL ,
	[DAY_NAME_ABREV]     char(3)  NOT NULL ,
	[DAY_OF_MONTH]       tinyint  NOT NULL ,
	[DAY_OF_MONTH_SUFFIX] char(2)  NOT NULL ,
	[DAY_OF_YEAR]        smallint  NOT NULL ,
	[WEEK_OF_MONTH]      tinyint  NOT NULL ,
	[WEEK_OF_YEAR]       tinyint  NOT NULL ,
	--
	[MONTH_OF_YEAR]      tinyint  NOT NULL ,
	[MONTH_NAME]         varchar(15)  NOT NULL ,
	[MONTH_NAME_ABREV]   char(3)  NOT NULL ,
	[QUARTER_OF_YEAR]    tinyint  NOT NULL ,
	[YEAR]               smallint  NOT NULL ,
	--
	[WEEKDAY_IND]        bit  NOT NULL,
    [HOLIDAY_IND]        bit  NOT NULL ,
	[ROW_EFFECTIVE_DATE] date  NOT NULL ,
	[ROW_EXPIRATION_DATE] date  NOT NULL ,
	[LAST_UPDATE_USER]   varchar(25)  NOT NULL ,
	[SRC_SYSTEM_CODE]    varchar(10)  NOT NULL ,
	[INSERT_BATCH_SK]    bigint  NOT NULL ,
	[UPDATE_BATCH_SK]    bigint  NOT NULL 
)
go

ALTER TABLE xdm.[DIM_DATE]
	ADD CONSTRAINT [XPKDIM_DATE] PRIMARY KEY  CLUSTERED ([DATE_SK] ASC)
go
*/
-- DEFAULT ROW
--  insert into xdm.dim_date values (-1, cast ('1899-12-31' as date), 0, '', '', 0, '', 0, 0, 0, 0, '', '', 0, 0, 0, 0, cast( '1900-01-01' as date ), cast( '2999-12-31' as date), replace(system_user, 'GODDARDSYSTEMS\', ''), '', -1, -1 )

--  drop table xdm.[DIM_DATE]
--  delete from [XDM].[DIM_DATE] where school_market_sk <> -1
--  select * from xdm.dim_date



-- INSERT NEW ROWS

declare @BATCH_SK bigint
declare @RET_VAL integer
declare @INS_COUNT bigint = 0
declare @UPD_COUNT bigint = 0
declare @DEL_COUNT bigint = 0
declare @MSG varchar(500)
declare @TMP_DATE date = '1900-01-01'
declare @END_DATE date = '2099-12-31'

-- Get new Batch SK
exec xdm.sp_new_batch 'LOAD: DIM_DATE', @BATCH_SK out, @RET_VAL out
if @RET_VAL = 1 return


--- Empty table except for default row ---
delete from xdm.dim_date where date_sk <> -1


--- insert new records ---
while @TMP_DATE < @END_DATE
begin

begin try

   insert into xdm.dim_date
   select year( @TMP_DATE ) * 10000 + month( @TMP_DATE ) * 100 + day( @TMP_DATE ) as DATE_SK,
   @TMP_DATE                                                                      as DATE_VALUE,
   datepart( dw, @TMP_DATE )                                                      as DAY_OF_WEEK,
   datename( dw, @TMP_DATE )                                                      as DAY_NAME,
   upper( left( datename( dw, @TMP_DATE ), 3 ))                                   as DAY_NAME_ABREV,
   day(@TMP_DATE)                                                                 as DAY_OF_MONTH,
   
   case when day( @TMP_DATE ) in ( 1, 21, 31 ) then 'st'
        when day( @TMP_DATE ) in ( 2, 22 )     then 'nd'
        when day( @TMP_DATE ) in ( 3, 23 )     then 'rd'
        else 'th'
   end                                                                            as DAY_OF_MONTH_SUFFIX,
   
   datename( dy, @TMP_DATE )                                                      as DAY_OF_YEAR,
   datepart( week, @TMP_DATE) - datepart( week, dateadd( MM, datediff( MM, 0, @TMP_DATE), 0 )) + 1 as WEEK_OF_MONTH,
   datepart( wk, @TMP_DATE)                                                       as WEEK_OF_YEAR,
   --
   month( @TMP_DATE )                                                             as MONTH_OF_YEAR,
   datename( mm, @TMP_DATE )                                                      as MONTH_NAME,
   upper( left( datename( mm, @TMP_DATE), 3 ))                                    as MONTH_NAME_ABREV,
   datepart( q, @TMP_DATE)                                                        as QUARTER_OF_YEAR,
   year( @TMP_DATE ) as [YEAR],
   case when datepart( dw, @TMP_DATE ) in ( 2, 3, 4, 5, 6 ) then 1 else 0 end     as WEEKDAY_IND,
   
   case when month( @TMP_DATE ) = 1 and day( @TMP_DATE ) = 1 then 1
        when month( @TMP_DATE ) = 7 and day( @TMP_DATE ) = 4 then 1
		when month( @TMP_DATE ) = 12 and day( @TMP_DATE ) = 25 then 1
		else 0
   end                                                                            as HOLIDAY_IND,
   
   cast( '1900-01-01' as date )                                                   as ROW_EFFECTIVE_DATE,
   cast( '2999-12-31' as date )                                                   as ROW_EXPIRATION_DATE,
	      replace(system_user, 'GODDARDSYSTEMS\', '')                             as LAST_UPDATE_USER,
	      'DERIVED'                                                               as SRC_SYSTEM_CODE,
	      @BATCH_SK                                                               as INSERT_BATCH_SK,
		  -1                                                                      as UPDATE_BATCH_SK

end try
begin catch

   set @MSG = error_message()
   exec xdm.sp_update_batch @BATCH_SK, 'F', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out
   return

end catch

   set @TMP_DATE = dateadd( DD, 1, @TMP_DATE )
   set @INS_COUNT = @INS_COUNT + 1

end

exec xdm.sp_update_batch @BATCH_SK, 'S', @INS_COUNT, @UPD_COUNT, @DEL_COUNT, @MSG, @RET_VAL out


-- select * from xdm.dim_date
-- select top 10 * from xdm.dim_batch_audit order by 1 desc


