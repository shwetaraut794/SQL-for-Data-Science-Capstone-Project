USE [SportsStats]
GO
/****** Object:  StoredProcedure [dbo].[insert_into_Sport]    Script Date: 6/19/2022 11:27:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   procedure [dbo].[insert_into_Sport]
(
	@Sport_data_frame dbo.udt_Sport readonly
)
AS

BEGIN
	Declare @SportTempTable TABLE (Act VARCHAR(32), tbl_id int, udt_id int);
	
	Merge Sport as tar
	using @Sport_data_frame as src
	on (tar.Sport_id = src.Sport_id)
	when matched then 
		update set tar.Sport_id = src.Sport_id, tar.Sports = src.Sports
	when not matched then
		insert(Sport_id, Sports) 
		values (src.Sport_id, src.Sports)
	output $Action, inserted.Sport_Id as TarId, src.Sport_id as SrcId into @SportTempTable (Act, tbl_id, udt_id);
	
	select tbl_id as 'TarId', udt_id as 'SrcId' from @SportTempTable order by udt_id
END;
