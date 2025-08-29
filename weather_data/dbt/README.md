# weather_dbt

This is a minimal dbt project scaffold.

Directories:
- models/: SQL models and YAML (sources, tests)
- analyses/: ad-hoc queries
- macros/: custom macros
- seeds/: CSV data files to seed
- snapshots/: snapshot definitions
- tests/: generic or data tests

Run:
- dbt debug
- dbt run
- dbt test
