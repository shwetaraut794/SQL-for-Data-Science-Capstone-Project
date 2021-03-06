USE [SportsStats]
GO
/****** Object:  StoredProcedure [dbo].[insert_into_Team]    Script Date: 6/19/2022 11:27:47 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   procedure [dbo].[insert_into_Team]
(
	@Team_data_frame dbo.udt_Team readonly
)
AS

BEGIN
	Declare @TeamTempTable TABLE (Act VARCHAR(32), tbl_id int, udt_id int);
	
	Merge Team as tar
	using @Team_data_frame as src
	on (tar.Team_id = src.Team_id)
	when matched then 
		update set tar.Team_id = src.Team_id, tar.Teams = src.Teams
	when not matched then
		insert(Team_id, Teams, NOC_id) 
		values (src.Team_id, src.Teams, src.NOC_id)
	output $Action, inserted.Team_Id as TarId, src.Team_id as SrcId into @TeamTempTable (Act, tbl_id, udt_id);
	
	select tbl_id as 'TarId', udt_id as 'SrcId' from @TeamTempTable order by udt_id
END;
