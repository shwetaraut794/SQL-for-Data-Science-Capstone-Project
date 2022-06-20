USE SportsStats
GO

CREATE TYPE udt_GameDetail AS TABLE 
(
	Gamed_id int,
	Game_id int,
	E_id int
)
GO

create or alter procedure dbo.[insert_into_GameDetail]
(
	@GameDetail_data_frame dbo.udt_GameDetail readonly
)
AS

BEGIN
	Declare @GameDetailTempTable TABLE (Act VARCHAR(32), tbl_id int, udt_id int);
	
	Merge GameDetail as tar
	using @GameDetail_data_frame as src
	on (tar.Gamed_id = src.Gamed_id)
	when matched then 
		update set tar.Gamed_id = src.Gamed_id, tar.Game_id= src.Game_id
	when not matched then
		insert(Gamed_id, Game_id, E_id) 
		values (src.Gamed_id, src.Game_id, src.E_id)
	output $Action, inserted.Gamed_Id as TarId, src.Gamed_id as SrcId into @GameDetailTempTable (Act, tbl_id, udt_id);
	
	select tbl_id as 'TarId', udt_id as 'SrcId' from @GameDetailTempTable order by udt_id
END;
