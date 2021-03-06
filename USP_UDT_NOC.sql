USE [SportsStats]
GO
/****** Object:  StoredProcedure [dbo].[insert_into_NOC]    Script Date: 6/19/2022 11:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   procedure [dbo].[insert_into_NOC]
(
	@NOC_data_frame dbo.udt_NOC readonly
)
AS

BEGIN
	Declare @NOCTempTable TABLE (Act VARCHAR(32), tbl_id int, udt_id int);
	
	Merge NOC as tar
	using @NOC_data_frame as src
	on (tar.NOC_id = src.NOC_id)
	when matched then 
		update set tar.NOC_id = src.NOC_id, tar.NOC = src.NOC
	when not matched then
		insert(NOC_id, NOC, Region) 
		values (src.NOC_id, src.NOC, src.Region)
	output $Action, inserted.NOC_Id as TarId, src.NOC_id as SrcId into @NOCTempTable (Act, tbl_id, udt_id);
	
	select tbl_id as 'TarId', udt_id as 'SrcId' from @NOCTempTable order by udt_id
END;
