USE [SportsStats]
GO
/****** Object:  StoredProcedure [dbo].[insert_into_Game]    Script Date: 6/19/2022 11:27:12 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   procedure [dbo].[insert_into_Game]
(
	@Game_data_frame dbo.udt_Game readonly
)
AS

BEGIN
	Declare @GameTempTable TABLE (Act VARCHAR(32), tbl_id int, udt_id int);
	
	Merge Game as tar
	using @Game_data_frame as src
	on (tar.Game_id = src.Game_id)
	when matched then 
		update set tar.Game_id = src.Game_id, tar.Year= src.Year
	when not matched then
		insert(Game_id, Year, Season) 
		values (src.Game_id, src.Year, src.Season)
	output $Action, inserted.Game_Id as TarId, src.Game_id as SrcId into @GameTempTable (Act, tbl_id, udt_id);
	
	select tbl_id as 'TarId', udt_id as 'SrcId' from @GameTempTable order by udt_id
END;