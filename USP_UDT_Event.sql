USE [SportsStats]
GO
/****** Object:  StoredProcedure [dbo].[insert_into_Event]    Script Date: 6/19/2022 11:26:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   procedure [dbo].[insert_into_Event]
(
	@Event_data_frame dbo.udt_Event readonly
)
AS

BEGIN
	Declare @EventTempTable TABLE (Act VARCHAR(32), tbl_id int, udt_id int);
	
	Merge Event as tar
	using @Event_data_frame as src
	on (tar.E_id = src.E_id)
	when matched then 
		update set tar.E_id = src.E_id, tar.Events = src.Events
	when not matched then
		insert(E_id, Events) 
		values (src.E_id, src.Events)
	output $Action, inserted.E_Id as TarId, src.E_id as SrcId into @EventTempTable (Act, tbl_id, udt_id);
	
	select tbl_id as 'TarId', udt_id as 'SrcId' from @EventTempTable order by udt_id
END;
