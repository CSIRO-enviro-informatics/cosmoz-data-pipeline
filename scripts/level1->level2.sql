USE [CosmOz]
GO

/****** Object:  View [dbo].[Level2View]    Script Date: 8/02/2019 3:37:33 PM ******/
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
l = Level1View View
a = AllStations Table
s = SiloData Table
i = Intensity Table

===========
  EXAMPLE
===========
l.SiteNo	= SiteNo from the Level1View View for this row.
i.Intensity = Intensity from the Intensity Table for this row.



Special Names:
**************
Pressure		= Pressure2 (from Level1View View) IF Pressure is not zero (0)
					OR Pressure1 (from Level1View View)
Temperature		= ExternalTemperature (from Level1View View) IF ExternalTemperature AND ExternalHumidity are NOT zero (0)
					OR AverageTemperature (from SiloData Table) IF AverageTemperature AND AverageHumidity are NOT NULL
Humidity		= ExternalHumidity (from Level1View View) IF ExternalTemperature AND ExternalHumidity are NOT zero (0)
					OR AverageHumidity (from SiloData Table) IF AverageTemperature AND AverageHumidity are NOT NULL
IntensityValue	= Intensity (from Intensity Table) IF timestamp matched TO THE HOUR is NOT NULL
					OR last valid Intensity (from Intensity Table) for the site
					OR first valid Intensity (from Intensity Table)



Equations:
**********
SeaLevelRefPressure					= 1013.25
AtmosphericAttenuationCoefficient	= a.Beta
satVp								= 0.6108*exp((17.27*Temperature)/(Temperature+237.3)
Vp									= satVp*(Humidity/100)
AbsoluteHumidity					= (2165*Vp)/(Temperature+273.16)
AtmosphericDepth					= a.RefPressure/(9.80655/10)
SeaLevelAtmosphericDepth			= SeaLevelRefPressure/(9.80655/10)
ElevationScaling					= a.ElevScaling
LatitudeScaling						= a.LatitScaling

SiteNo			= l.SiteNo
Timestamp		= l.Timestamp
Count			= l.Count
PressCorr		= exp(a.Beta*(Pressure-a.RefPressure))
WVCorr			= 1+0.0054*(AbsoluteHumidity-0)
IntensityCorr	= IntensityValue/a.RefIntensity
CorrCount		= ((l.Count*PressCorr*WVCorr)/IntensityCorr)/(LatitudeScaling/ElevationScaling)
Flag			= l.Flag


Substituted Equations:
**********************
SiteNo			= l.SiteNo
Timestamp		= l.Timestamp
Count			= l.Count
PressCorr		= exp(a.Beta*(Pressure-a.RefPressure))
WVCorr			= 1+0.0054*(AbsoluteHumidity-0)
				= 1+0.0054*((2165*((0.6108*EXP((17.27*Temperature)/(Temperature+237.3)))*(Humidity/100)))/(Temperature+273.16)-0)
IntensityCorr	= IntensityValue / a.RefIntensity
CorrCount		= ((l.Count*PressCorr*WVCorr)/intensityCorr)/(LatitudeScaling/ElevationScaling)
				= ((l.Count*(EXP(a.Beta*(Pressure-a.RefPressure)))*(1+0.0054*((2165*((0.6108*EXP((17.27*Temperature)/(Temperature+237.3)))*(Humidity/100)))/(Temperature+273.16)-0)))/(IntensityValue / a.RefIntensity))/((1-EXP(-9.694*POWER(a.CutoffRigidity,-0.9954)))/(EXP(((a.RefPressure/(9.80655/10))-(1013.25/(9.80655/10)))/(1/a.Beta))))


Additional Notes:
*****************
EXP(x)			= SQL Server's version of e^x where "e" is "Euler's constant"
POWER(x, y)		= SQL Server's version of x^y
WVCorr			= 1 IF no valid values are found for Temperature OR Humidity
IntensityCorr	= 1 IF no valid value is found for IntensityValue
*/

CREATE VIEW [dbo].[Level2View]
AS
  SELECT
	l.SiteNo AS SiteNo,
	l.Timestamp AS Timestamp,
	l.[Count] AS [Count],
	CASE
		WHEN(Pressure2 <> 0)
			THEN EXP((a.Beta)*((l.Pressure2)-(a.RefPressure)))
		ELSE
			EXP((a.Beta)*((l.Pressure1)-(a.RefPressure)))
	END AS PressCorr,
	CASE
		--IF ExternalTemperature AND ExternalHumidity (from the Level1View View) has valid data. Use them in the VWCorr equation
		WHEN(ExternalTemperature <> 0 AND ExternalHumidity<> 0)
			THEN (1+0.0054*((2165*((0.6108*EXP((17.27*(l.ExternalTemperature))/((l.ExternalTemperature)+237.3)))*((l.ExternalHumidity)/100)))/((l.ExternalTemperature)+273.16)-0))
		--Otherwise, IF AverageTemperature AND AverageHumidity (from the SiloData table) has valid data. Use them in the VWCorr equation.
		WHEN ( (SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)) IS NOT NULL AND (SELECT AverageHumidity FROM dbo.SiloData AS s WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)) IS NOT NULL )
			THEN (1+0.0054*((2165*((0.6108*EXP((17.27*((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2))))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+237.3)))*(((SELECT AverageHumidity FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))/100)))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+273.16)-0))
		--Otherwise (everything else has failed) default to 1
		ELSE
			1
	END AS WVCorr,
	CASE
		--IF we can match the record's timestamp (to the hour) to one in the Intensity table. Use the Intensity value in the IntensityCorr equation.
		WHEN ( (SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp) ) IS NOT NULL )
			THEN (SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp))/a.RefIntensity
		--Otherwise, IF we can find the last valid timestamp for this record. Use the Intensity value in the IntensityCorr equation.
		WHEN ( (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp < l.Timestamp ORDER BY i.Timestamp ) IS NOT NULL )
			THEN (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp <= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity
		--Otherwise, IF we can find the first valid timestamp for this record (for records that exist before NMDB supplied an intensity value). Use the Intensity value in the IntensityCorr equation.
		WHEN ( (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ) IS NOT NULL )
			THEN (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity
		--Otherwise (everything else has failed) default to 1
		ELSE
			1
	END AS IntensityCorr
	,CASE
		--IF ExternalHumidity (from the Level1View View) has valid data...
		WHEN ( ExternalHumidity <> 0 )
		THEN CASE
			--IF we can match the record's timestamp (to the hour) to one in the Intensity table. Use the Intensity value AND ExternalHumidity in the CorrCount equation.
			WHEN ( (SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp) ) IS NOT NULL )
			THEN CASE
				WHEN (Pressure2 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*(l.ExternalTemperature))/((l.ExternalTemperature)+237.3)))*((l.ExternalHumidity)/100)))/((l.ExternalTemperature)+273.16)-0)))*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp))/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				WHEN (Pressure1 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*(l.ExternalTemperature))/((l.ExternalTemperature)+237.3)))*((l.ExternalHumidity)/100)))/((l.ExternalTemperature)+273.16)-0)))*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp))/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				ELSE
					1
				END
			--Otherwise, IF we can find the last valid timestamp for this record. Use the Intensity value AND ExternalHumidity in the CorrCount equation.
			WHEN ( (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp < l.Timestamp ORDER BY i.Timestamp ASC ) IS NOT NULL )
			THEN CASE
				WHEN (Pressure2 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*(l.ExternalTemperature))/((l.ExternalTemperature)+237.3)))*((l.ExternalHumidity)/100)))/((l.ExternalTemperature)+273.16)-0)))*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp <= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				WHEN (Pressure1 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*(l.ExternalTemperature))/((l.ExternalTemperature)+237.3)))*((l.ExternalHumidity)/100)))/((l.ExternalTemperature)+273.16)-0)))*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp <= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				ELSE
					1
				END
			--Otherwise, IF we can find the first valid timestamp for this record (for records that exist before NMDB supplied an intensity value). Use the Intensity value AND ExternalHumidity in the CorrCount equation.
			WHEN ( (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC ) IS NOT NULL )
			THEN CASE
				WHEN (Pressure2 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*(l.ExternalTemperature))/((l.ExternalTemperature)+237.3)))*((l.ExternalHumidity)/100)))/((l.ExternalTemperature)+273.16)-0)))*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				WHEN (Pressure1 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*(l.ExternalTemperature))/((l.ExternalTemperature)+237.3)))*((l.ExternalHumidity)/100)))/((l.ExternalTemperature)+273.16)-0)))*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				ELSE
					1
				END
			--Otherwise (everything else has failed) default to 1
			ELSE
				1
			END
		--Otherwise, IF AverageHumidity in the SiloData table has valid data...
		WHEN ( (SELECT AverageHumidity FROM dbo.SiloData AS s WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)) IS NOT NULL )
		THEN CASE
			--IF we can match the record's timestamp (to the hour) to one in the Intensity table. Use the Intensity value AND AverageHumidity and AverageTemperature in the CorrCount equation.
			WHEN ( (SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp) ) IS NOT NULL )
			THEN CASE
				WHEN (Pressure2 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2))))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+237.3)))*(((SELECT AverageHumidity FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))/100)))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+273.16)-0)))*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp))/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				WHEN (Pressure1 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2))))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+237.3)))*(((SELECT AverageHumidity FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))/100)))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+273.16)-0)))*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp))/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				ELSE
					1
				END
			--Otherwise, IF we can find the last valid timestamp for this record. Use the Intensity value AND AverageHumidity in the CorrCount equation.
			WHEN ( (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp < l.Timestamp ORDER BY i.Timestamp ASC ) IS NOT NULL )
			THEN CASE
				WHEN (Pressure2 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2))))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+237.3)))*(((SELECT AverageHumidity FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))/100)))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+273.16)-0)))*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp <= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				WHEN (Pressure1 <> 0)
					THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2))))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+237.3)))*(((SELECT AverageHumidity FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))/100)))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+273.16)-0)))*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp <= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
				ELSE
					1
				END
			--Otherwise, IF we can find the first valid timestamp for this record (for records that exist before NMDB supplied an intensity value). Use the Intensity value AND AverageHumidity in the CorrCount equation.
			WHEN ( (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC ) IS NOT NULL )
				THEN CASE
					WHEN (Pressure2 <> 0)
						THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2))))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+237.3)))*(((SELECT AverageHumidity FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))/100)))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+273.16)-0)))*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
					WHEN (Pressure1 <> 0)
						THEN (l.Count*((1+0.0054*((2165*((0.6108*EXP((17.27*((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2))))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+237.3)))*(((SELECT AverageHumidity FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))/100)))/(((SELECT AverageTemperature FROM dbo.SiloData AS s  WHERE l.SiteNo = s.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, s.Date2)))+273.16)-0)))*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
					ELSE
						1
					END
			--Otherwise (everything else has failed) default to 1
			ELSE
				1
			END
		--Otherwise (we could not use ExternalHumidity OR AverageHumidity). We use 1 = 1 to force it to run this 'failsafe case'
		WHEN (1 = 1)
		THEN CASE
			--IF we can match the record's timestamp (to the hour) to one in the Intensity table. Use the Intensity value AND AverageHumidity and AverageTemperature in the CorrCount equation.
			WHEN ( (SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp) ) IS NOT NULL )
				THEN CASE
					WHEN (Pressure2 <> 0)
						THEN (l.Count*(1)*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp))/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
					WHEN (Pressure1 <> 0)
						THEN (l.Count*(1)*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT i.Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND CONVERT(date, l.Timestamp) = CONVERT(date, i.Timestamp) AND DATEPART(HOUR, l.Timestamp) = DATEPART(HOUR, i.Timestamp))/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
					ELSE
						1
					END
			--Otherwise, IF we can find the last valid timestamp for this record. Use the Intensity value AND AverageHumidity in the CorrCount equation.
			WHEN ( (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp < l.Timestamp ORDER BY i.Timestamp ASC ) IS NOT NULL )
				THEN CASE
					WHEN (Pressure2 <> 0)
						THEN (l.Count*(1)*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp <= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
					WHEN (Pressure1 <> 0)
						THEN (l.Count*(1)*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp <= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
					ELSE
						1
					END
			--Otherwise, IF we can find the first valid timestamp for this record (for records that exist before NMDB supplied an intensity value). Use the Intensity value AND AverageHumidity in the CorrCount equation.
			WHEN ( (SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC ) IS NOT NULL )
				THEN CASE
					WHEN (Pressure2 <> 0)
						THEN (l.Count*(1)*(EXP((a.Beta)*((l.Pressure2)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
					WHEN (Pressure1 <> 0)
						THEN (l.Count*(1)*(EXP((a.Beta)*((l.Pressure1)-(a.RefPressure))))/((SELECT TOP 1 Intensity FROM dbo.Intensity AS i WHERE l.SiteNo = i.SiteNo AND i.Timestamp >= l.Timestamp ORDER BY i.Timestamp ASC)/a.RefIntensity))/((a.LatitScaling)/(a.ElevScaling))
					ELSE
						1
					END
			ELSE
				1
			END
	END AS CorrCount,
	l.[Flag]
  FROM dbo.AllStations AS a, dbo.Level1View AS l
  WHERE a.SiteNo = l.SiteNo
GO


