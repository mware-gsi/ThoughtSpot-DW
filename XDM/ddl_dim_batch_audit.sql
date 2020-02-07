
----------------------------------------------------------
--
-- Filename: ddl_dim_batch_audit.sql
--
-- Author:        M. Ware
-- Last Rev Date: 2020-01-23
--
--  Description:  creates table DIM_BATCH_AUDIT
--
----------------------------------------------------------

use FMS_PROFILE


CREATE TABLE [XDM].[DIM_BATCH_AUDIT](
	[BATCH_SK] [bigint] NOT NULL,
	[BATCH_DESC] [varchar](100) NOT NULL,
	[START_DT] [datetime] NOT NULL,
	[END_DT] [datetime] NULL,
	[STATUS_CODE] char(1) NOT NULL,
	[ROW_INSERT_COUNT] [bigint] NULL,
	[ROW_UPDATE_COUNT] [bigint] NULL,
	[ROW_DELETE_COUNT] [bigint] NULL,
	[MESSAGE] varchar(500)
) 

ALTER TABLE [XDM].[DIM_BATCH_AUDIT]
	ADD CONSTRAINT [XPKDIM_BATCH_AUDIT] PRIMARY KEY  CLUSTERED ([BATCH_SK] ASC)

--  drop table xdm.dim_batch_audit
--  truncate table xdm.dim_batch_audit

--  insert into xdm.dim_batch_audit values (-1, 'DEFAULT BATCH', '1900-01-01', '1900-01-01', 'S',0, 0, 0, '' )



--  select * from xdm.dim_batch_audit order by batch_sk desc