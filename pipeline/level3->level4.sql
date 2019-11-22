USE [CosmOz]
GO

/****** Object:  View [dbo].[Level4View]    Script Date: 8/02/2019 3:39:18 PM ******/
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
l3 = Level3View View

===========
  EXAMPLE
===========
l3.Rainfall = Rainfall from the Level3View View for this row. 



Special Names:
**************
None.



Equations:
**********
SiteNo				= l3.SiteNo
Timestamp			= l3.Timestamp
SoilMoist			= l3.SoilMoist
EffectiveDepth		= l3.EffectiveDepth
Rainfall			= l3.Rainfall
SoilMoistFiltered	= [7 hour moving average of valid l3.SoilMoist]
DepthFiltered		= [7 hour moving average of valid l3.EffectiveDepth]



Substituted Equations:
**********************
None...


Additional Notes:
*****************
All 'flagged' data and data before the installation date of the site are excluded at this level
The 7 hour moving average is calculated with the past 3 hours of valid records, the current record and 
the next 3 hours of valid records.
	- Invalid (flagged) records are ignored by the moving average!
*/
CREATE VIEW [dbo].[Level4View]
AS
	SELECT SiteNo, Timestamp, SoilMoist, EffectiveDepth, Rainfall, 
		dbo.SoilMoistureMovingAverage(SiteNo, Timestamp) AS SoilMoistFiltered, 
		dbo.EffectiveDepthMovingAverage(SiteNo, Timestamp) AS DepthFiltered
	FROM Level3View AS l3
	WHERE Flag = 0
	AND Timestamp >= (SELECT InstallationDate FROM AllStations WHERE SiteNo = l3.SiteNo)

GO
