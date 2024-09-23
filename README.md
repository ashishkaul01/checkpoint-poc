# checkpoint-poc

create_data.py file will create the deltatable and append data to it if the table already exists.
main.py file will read the change data feed (CDF) and get the max version from it. It would then save the max version to a file and eventually read from that file the max version as starting version for the next run. thus ensuring we never read duplicate changes.

## Steps to Use

- docker-compose up -d
- install dependencies using poetry
- poetry run python create_data.py (run twice)
- poetry run python main.py
  - Iniitally starting_version = 0, cdf_length = 1 and cdf_version = 0
- poetry run python create_data.py (run twice)
- poetry run python main.py
  - starting_version = 0 cdf_length = 3 and cdf_version = 2
- poetry run python main.py
  - starting_version = 2 cdf_length = 1 and cdf_version = 2
