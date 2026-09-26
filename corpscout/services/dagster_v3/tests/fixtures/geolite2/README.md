# GeoLite2 test databases

`GeoLite2-City-Test.mmdb` and `GeoLite2-ASN-Test.mmdb` are MaxMind's official test
databases, copied unchanged from the MaxMind-DB repository:

- https://github.com/maxmind/MaxMind-DB/tree/0eef25a46e20f4e96d27b951d0228efabe21323f/test-data
- commit `0eef25a46e20f4e96d27b951d0228efabe21323f` (downloaded 2026-09-26)
- licence: (c) MaxMind, Inc.; the repository README (same commit) licenses its contents
  under the Apache License 2.0 or the MIT License, at your option. (The task brief said
  CC BY-SA; the repository states Apache-2.0/MIT.)

Their `database_type` is `GeoLite2-City` / `GeoLite2-ASN` and their `build_epoch` is
1770245369. `tests/test_geolite2_install.py` rewrites the 4-byte `build_epoch` in the
metadata section to produce older and newer builds, and builds the `.tar.gz` archives
itself.
