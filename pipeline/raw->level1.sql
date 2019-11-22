USE "CosmOz"
GO

/****** Object:  View "dbo"."Level1View"    Script Date: 8/02/2019 3:35:36 PM ******/
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
N/A

===========
  EXAMPLE
===========
N/A



Special Names:
**************
N/A



Equations:
**********
SiteNo					= RawData.SiteNo
Timestamp				= RawData.Timestamp
Count					= RawData.Count
Pressure1				= RawData.Pressure1
InternalTemperature		= RawData.InternalTemperature
InternalHumidity		= RawData.InternalHumidity
Battery					= RawData.Battery
TubeTemperature			= RawData.TubeTemperature
TubeHumidity			= RawData.TubeHumidity
Rain					= RawData.Rain
VWC1					= RawData.VWC1
VWC2					= RawData.VWC2
VWC3					= RawData.VWC3
Pressure2				= RawData.Pressure2
ExternalTemperature		= RawData.ExternalTemperature
ExternalHumidity		= RawData.ExternalHumidity
Flag					= 4 "IF RawData.Battery < 10" OR
						= 1 "IF Current Count is 20% different to previous Count" OR
						= RawData.Flag



Substituted Equations:
**********************
N/A



Additional Notes:
*****************
LAG("Count") means to check the previous record's Count value
*/

--CREATE VIEW "dbo"."Level1View"
CREATE CONTINUOUS QUERY level1_view ON cosmoz
BEGIN
SELECT "SiteNo"
      ,"Timestamp"
      ,"Count"
      ,"Pressure1"
      ,"InternalTemperature"
      ,"InternalHumidity"
      ,"Battery"
      ,"TubeTemperature"
      ,"TubeHumidity"
      ,"Rain"
      ,"VWC1"
      ,"VWC2"
      ,"VWC3"
      ,"Pressure2"
      ,"ExternalTemperature"
      ,"ExternalHumidity"
	  ,
		CASE
			WHEN ("Battery" < 10)
				THEN 4
			WHEN ("Count" < (0.8 * ((DIFFERENCE("Count") * -1.0)+"Count")) OR "Count" > (1.2 * ((DIFFERENCE("Count") * -1.0)+"Count")))
			--WHEN ("Count" < (0.8 * LAG("Count") OVER(ORDER BY SiteNo, Timestamp ASC)) OR "Count" > (1.2 * LAG("Count") OVER(ORDER BY SiteNo, Timestamp ASC)))
				THEN 1
			ELSE
				"RawData"."Flag"
		END AS "Flag"
  FROM "dbo"."RawData"

GO
