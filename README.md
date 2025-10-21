# NFDRS Percentiles and Trends from FEMS

The previous NFDRS Percentiles and Trends service was revamped to accommodate the transition from the Weather Information Management System (WIMS) to the Fire Environment Mapping System (FEMS) for Fire Danger Information in October of 2025.

Service: https://nifc.maps.arcgis.com/home/item.html?id=db8921ba6baa4444bd867aab3e69260e

**Purpose**

This code completes an automated pull of observed and forecasted fire danger data for Remote Automated Weather Stations (RAWS) from the Fire Environment Mapping System (FEMS), and then translates Energy Release Component (ERC) and Burning Index (BI) values into percentiles, analyzes observed and forecasted trends for each index, and aggregates the same information up to the Predictive Service Area (PSA) level. The resulting spatial web service is intended to provide a high-level view of recently observed and forecasted fire danger for national and geographic area decision makers.

**Input data**
- Static copy of Remote Automated Weather Station (RAWS) locations from FEMS station metadata (last updated 09/2025).
- Static copy of Predictive Service Area (PSA) boundaries from the National Interagency Fire Center (https://data-nifc.opendata.arcgis.com/datasets/nifc::predictive-service-area-psa-boundaries-public/about) (last updated 09/2025).
- Tables of key RAWS associated with each PSA and the historical percentiles of Energy Release Component (ERC) and Burning Index (BI). The initial tables (last updated 09/2025) were generated using the gap-filled historical daily fire danger extremes for fuel model Y (also known as “2016 forest”) from the Fire Environment Mapping System (FEMS) for the period 2005-2022 with no data manipulation. Historical percentiles were determined using the full calendar year.
- Daily fire danger extremes from the Fire Environment Mapping System (FEMS) accessed at 03:00 Mountain Time: 1) most recent 3 days of observed fire danger; and 2) next 3 days of forecasted fire danger.

**Analysis**
- The most recent day of observed and the next forecasted maximum fire danger indices are converted to percentiles based on the historical percentile tables.
- Trend analysis categories determined by: 1) observed uses most recent daily observation compared to two days prior; 2) forecasted uses current day forecast compared to two days in the future; and 3) increase (>= +3), decrease (<= -3), or no change (< 3 diff) based on difference in absolute ERC or BI values, not percentiles.
- Aggregation to PSA: 1) non-reporting stations are ignored in calculations; 2) the PSA is assigned a null value if it has no reporting stations, 3) simple means of RAWS percentiles; and 4) trends determined using simple means of index values from associated RAWS for equivalent time periods and same change thresholds (see above).

