USE [SportsStats]
GO
/****** Object:  StoredProcedure [dbo].[insert_into_athlete]    Script Date: 6/19/2022 11:26:01 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   procedure [dbo].[insert_into_athlete]
(
	@athlete_data_frame dbo.udt_Athletedata readonly
)
AS

BEGIN
	Declare @AthleteTempTable TABLE (Act VARCHAR(32), tbl_id int, udt_id int);
	--Declare @use_team_id int
	--SET @use_team_id = (select team.team_id from team where team.Teams = Teams);
	
	
	Merge Athlete as tar
	using @athlete_data_frame as src
	on (tar.A_id = src.A_id)
	when matched then 
		update set tar.A_id = src.A_id, tar.name = src.name
	when not matched then
		insert(A_id, Team_id_fk, Name, Sex, Age, Weight, Height, Medal) 
		values (src.A_id, src.Team_id_fk, src.Name, src.Sex, src.Age, src.Weight, src.Weight, src.Medal)
	output $Action, inserted.A_Id as TarId, src.A_id as SrcId into @AthleteTempTable (Act, tbl_id, udt_id);
	
	select tbl_id as 'TarId', udt_id as 'SrcId' from @AthleteTempTable order by udt_id
END;