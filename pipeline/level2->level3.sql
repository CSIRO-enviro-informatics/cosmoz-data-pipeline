USE [CosmOz]
GO

/****** Object:  View [dbo].[Level3View]    Script Date: 8/02/2019 3:38:15 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/* 
Keys and Example Usage:
***********************
========
  Keys
========
a = AllStations Table
l1 = Level1View View
l2 = Level2View View

===========
  EXAMPLE
===========
r.SiteNo	= SiteNo from the RawData Table for this row.
l2.CorrCount = CorrCount from the Level2View View for this row. 



Special Names:
**************
None.



Equations:
**********
SiteNo				= l2.SiteNo
Timestamp			= l2.Timestamp
SoilMoist			= ((0.0808 / ( (l2.CorrCount / a.N0_Cal) - 0.372) - 0.115 - a.LatticeWater_g_g - a.SoilOrganicMatter_g_g) * a.BulkDensity) * 100
EffectiveDepth		= 5.8 / ( ((a.LatticeWater_g_g + a.SoilOrganicMatter_g_g) * a.BulkDensity) + SoilMoist + 0.0829 )
Rainfall			= l1.Rain * 0.2
Flag				= 5 [IF l2.CorrCount = 1] OR
					= 3 [IF l2.CorrCount > a.N0_Cal] OR
					= 2 [IF l2.CorrCount < 40% of N0_Cal] OR
					= l2.Flag



Substituted Equations:
**********************
Effective Depth	= 5.8 / ( ((a.LatticeWater_g_g + a.SoilOrganicMatter_g_g) * a.BulkDensity) + SoilMoist + 0.0829 )
				= 5.8 / ( ((a.LatticeWater_g_g + a.SoilOrganicMatter_g_g) * a.BulkDensity) + ( (0.0808 / ( (l2.CorrCount / a.N0_Cal) - 0.372) - 0.115 - a.LatticeWater_g_g - a.SoilOrganicMatter_g_g) * a.BulkDensity ) + 0.0829 )



Additional Notes:
*****************
AVG(...)	= SQL Server's version of AVERAGE which ignores NULL values.
*/



CREATE VIEW [dbo].[Level3View]
AS
	SELECT 
		l2.SiteNo AS SiteNo, 
		l2.Timestamp AS Timestamp,
		((0.0808 / ( (l2.CorrCount / a.N0_Cal) - 0.372) - 0.115 - a.LatticeWater_g_g - a.SoilOrganicMatter_g_g) * a.BulkDensity) * 100 AS SoilMoist,
		5.8 / ( ((a.LatticeWater_g_g + a.SoilOrganicMatter_g_g) * a.BulkDensity) + ( (0.0808 / ( (l2.CorrCount / a.N0_Cal) - 0.372) - 0.115 - a.LatticeWater_g_g - a.SoilOrganicMatter_g_g) * a.BulkDensity ) + 0.0829) AS EffectiveDepth,
		l1.Rain * 0.2 AS Rainfall,
		CASE
			WHEN (l2.WVCorr = 1)
				THEN 5
			WHEN (l2.CorrCount > a.N0_Cal)
				THEN 3
			WHEN (l2.CorrCount < (0.4 * a.N0_Cal))
				THEN 2
			ELSE
				l2.[Flag]
		END AS Flag
	FROM dbo.AllStations as a, dbo.Level1View AS l1, dbo.Level2View AS l2
	WHERE a.SiteNo = l1.SiteNo AND l1.SiteNo = l2.SiteNo
	AND l1.Timestamp = l2.Timestamp


GO
