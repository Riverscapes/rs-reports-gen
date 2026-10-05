# Consolidate and improve report examples

pain points:

- duplicated files
- to use the report_launcher.py has to be in the example folder
- no context - about what makes them good examples for each report
- disconnected from the web report interface which has user input & different limits (see reportDefs.ts)

good - to keep:

- easy to pick real world examples when developing / testing
- repeatable tests
- a benefit of disconnection with web ui is we can test inputs locally before we impose those limits in the web ui

ideas:

- a single folder
- a manifest of some kind, containing additional info (but needs to be simple enough that maintaining this isn't too annoying)
- can harvest info from the geojson itself (area, max length, size in bytes, bounding box) - like web ui does
- allow picking from layers same as on web
- report_launcher allow picking json from anywhere on file system, not just from folder