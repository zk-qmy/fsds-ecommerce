# Schema Design — star schema, feature tables

**Full write-up:** [`b_schema_pipelines/SCHEMA_DESIGN.md`](../../b_schema_pipelines/SCHEMA_DESIGN.md)

Complements `gold_README.md` (this folder — the DBeaver visual proof) with the actual schema
design behind it: full Kimball star schema (5 dims, 3 facts, 1 OBT), feature-table design
(offline 90-day + streaming 60-min + point-in-time joins), refresh plan, and DP3's homepage
candidate-generation design (§9). See the linked doc for the complete table-by-table grain,
key, and column design.
