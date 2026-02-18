---
description: 'Report generation repo standards'
---

* Targets Python 3.12
* prefer newer style typing rather than import from typing module

# Domain Knowledge

Riverscapes are the integration of terrestrial and aquatic systems from headwaters to estuaries that provide habitat and ecosystem benefits when in good health.

The Riverscapes consortium has developed a family of network models that model and calculate attributes for polygon riverscape segments (Reference: https://tools.riverscapes.net/). Riverscapes are defined laterally by the valley bottom extents, and are segmented into reach segments (discrete geographic objects -DGOs) that are connected across a riverscape drainage network.

Most of the report data comes from the Riverscape Metric Engine (RME) tool which synthesizes metrics from all the other production grade tools and combines them into a single, holistic dataset for assessing riverscapes health.  

Watersheds are identified by a 2/4/6/8 or 12 digit HUC code (left padded with zeros). Smaller watersheds have more digits and are nested inside larger ones.  e.g. 08 contains 0801 contains 080103 etc.