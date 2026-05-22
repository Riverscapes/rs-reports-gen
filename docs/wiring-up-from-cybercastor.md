# Wiring up a report to use the inputs from cybercastor

The `fargate-xxx.sh` scripts do the work.

👉 See also `NEW_REPORTS.md` in monorepo

## What we get

### index.json

example

```json
{
  "id": "c75abd77-4efe-44e0-b560-ddbef9178d54",
  "createdAtTS": 1779481942851,
  "updatedAtTS": 1779481942851,
  "createdById": "2c21f7c5-a8f7-4cc0-aecf-00dc92462ff0",
  "status": "RUNNING",
  "progress": 0,
  "description": "Highlights the importance of preserving and restoring natural space for rivers, emphasizing how healthy floodplains and corridors support ecosystems, reduce risks, and sustain communities. It makes the case that giving rivers room to move is essential for both resilience and long-term prosperity.",
  "name": "Rivers Need Space Report",
  "parameters": {
    "unitSystem": "imperial"
  },
  "reportTypeId": "rivers-need-space",
  "ccJobId": "f2015d15-ba16-454a-a641-9c6f1be48097",
  "ccTaskId": "cc11fff3-b120-46c2-9dc0-5de3b81b3ac3",
  "ccLastStatusTS": 1779481979927,
  "statusMessage": "CREATED"
}
```

### input.geojson

```json
{"type":"FeatureCollection",
"features":[
  {"type":"Feature","id":"55000500001172-6721","properties":{"id":"55000500001172-6721","huc10":"1702001510","level_path":"55000500001172","seg_distance":6721,"huc12":"170200151008"},
  "geometry": {"type":"Polygon","coordinates":[[[-119.601,46.893],[-119.60045099258423,46.893],[-119.60045099258423,46.89278307249168],[-119.59905624389648,46.89278307249168],[-119.59843397140503,46.89314965718785],[-119.59802627563477,46.891668639627824],[-119.59821939468384,46.89158065709964],[-119.59869146347046,46.89158065709964],[-119.59869146347046,46.891492674427155],[-119.59905624389648,46.891492674427155],[-119.59905624389648,46.89139002779348],[-119.59950685501099,46.89139002779348],[-119.59950685501099,46.89130204480827],[-119.59989309310913,46.89130204480827],[-119.59989309310913,46.89121406167868],[-119.60025787353516,46.89121406167868],[-119.60045099258423,46.89102343106953],[-119.60072994232178,46.89093544748292],[-119.601,46.89093544748292],[-119.60156679153442,46.89130204480827],[-119.601,46.893]]]}},
  {"type":"Feature","id":"55000500001172-5291","properties":{"id":"55000500001172-5291"},"geometry":...]}},
  ...
]
}
```

### Environment variables

from: `/rs-reports-monorepo/packages/common-server/src/lib/Cybercastor.ts`

```ts
const taskEnv = {
    USER_ID: user.id,
    REPORT_ID: report.id,
    STAGE: ctx.config.stage.toUpperCase(),
    // Note: We use the same API key for contacting Cybercastor as we do from within Cybcercastor to update the data exchange
    API_TOKEN: ctx.config.cybercastor.apiToken,
    UNIT_SYSTEM: report.parameters?.unitSystem || 'SI', // default to metric
    INCLUDE_GEOMETRY: String(Boolean(report.parameters?.includeGeometries)).toLowerCase(), // default to false
    GENERATE_PBI: String(Boolean(report.parameters?.includePBI)).toLowerCase(), // default to false
    CLIMATE_ENGINE_API_KEY: ctx.config.climateEngine.apiKey,
  }
```

