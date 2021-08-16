bamboo-cli --folder Bamboo-files --entry election_house --year=2020 --force=True --output-db=clickhouse-database

bamboo-cli --folder Bamboo-files --entry house_compact --year=2020 --force=True --output-db=clickhouse-database

bamboo-cli --folder Bamboo-files --entry election_senate --year=2020 --force=True --ingest=True --output-db=clickhouse-database

bamboo-cli --folder Bamboo-files --entry election_president_state --year=2020 --force=True --output-db=clickhouse-database

bamboo-cli --folder Bamboo-files --entry election_president_county --force=True --alaska-table=False --output-db=clickhouse-database