USE [SportsStats]
GO
/****** Object:  StoredProcedure [dbo].[insert_into_EventDetail]    Script Date: 6/19/2022 11:26:43 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Create the data type
--CREATE TYPE udt_EventDetail AS TABLE 
--(
--	Ed_id int,
--	A_id int,
--	E_id int
--)
--GO

ALTER   procedure [dbo].[insert_into_EventDetail]
(
	@EventDetail_data_frame dbo.udt_EventDetail readonly
)
AS

BEGIN
	Declare @EventDetailTempTable TABLE (Act VARCHAR(32), tbl_id int, udt_id int);
	
	Merge EventDetail as tar
	using @EventDetail_data_frame as src
	on (tar.Ed_id = src.Ed_id)
	when matched then 
		update set tar.Ed_id = src.Ed_id, tar.A_id_fk = src.A_id
	when not matched then
		insert(Ed_id, A_id_fk, E_id_fk) 
		values (src.Ed_id, src.A_id, src.E_id)
	output $Action, inserted.Ed_Id as TarId, src.Ed_id as SrcId into @EventDetailTempTable (Act, tbl_id, udt_id);
	
	select tbl_id as 'TarId', udt_id as 'SrcId' from @EventDetailTempTable order by udt_id
END;
