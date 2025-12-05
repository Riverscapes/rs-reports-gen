---
description: 'Report generation repo standards'
---

* Targets Python 3.12
* prefer newer style typing rather than import from typing module

# domain knowledge

Watersheds are identified by a 2/4/6/8 or 12 digit HUC code (left padded with zeros). Smaller watersheds have more digits and are nested inside larger ones.  e.g. 08 contains 0801 contains 080103 etc. 