# DataUSA-Election Bamboo ETL
The ETL scripts for DataUSA Elections

## Steps to Execute
Clone this repository:
```
git clone https://github.com/Datawheel/DataUSA-Election
```

### Installing OpenFEC-Wrapper
Install the OpenFec-Wrapper by following steps in the repository's README: 
https://github.com/Datawheel/DataUSA-Election/blob/master/OpenFEC-Wrapper
 
### Installing nltk:
 
 ```
 import nltk
 ```
 
 ```
 nltk.download('punkt')
 ```

### Executing scripts:
From cmd or terminal, run the bamboo ETL using bamboo-cli from the Bamboo-files folder. Replace ENTRY_FILE with any of the python files in this folder and OUTPUT-DB-CONNECTOR with the desired output database connector name:
```
bamboo-cli --folder . --entry ENTRY_FILE --output-db=OUTPUT-DB-CONNECTOR
```
### Executing the electoral_college script
From cmd temrinal, run the bamboo ETL using bamboo-cli from bamboo-files folder. Replace the parameters in according the required values
```
bamboo-cli --folder . --entry electoral_college --output-db=OUTPUT-DB-CONNECTOR --connector=CONNECTOR-FILE-NAME --campaign-d="CANDIDATE_LASTNAME" --campaign-r="CANDIDATE_LASTNAME" --year=YEAR --col1_number=COLUMN-NUMBER-IN-FEC-FOR-VICTORY --col2_number=COLUMN-NUMBER-IN-FEC-FOR-ELECTORAL-VOTES
```

### Update Procedure

In this section we provide information about the update procedure for the following cubes: `election_house`, `election_president`, `election_senate`, `election_electoralcollege`, and `election_house_compact`. These cubes are the ones commonly updated as new releases are being made.

First of all, make sure to install the repository `requirements.txt` file (`pip install -r requirements.txt`), and all the dependencies of `OpenFEC-Wrapper`. You may have an issue with `Pandas`, so it's better to install dependencies manually.

Secondly, it's important to know that all pipelines retrive information automatically. This means: when running a pipeline, we are actually downloading the data from the source, and running the ETL process, to ingest directly into our databases. As we can see, no data need to be downloaded manually. However, what we need to really be cautious about is to update the API's defined in `conns.yaml` file inside `Bamboo-files` directory. For these, we may need to access the site, and try downloading the corresponding file with the inspector opened, so we can see to which link we are requesting the data.

Thirdly, after updating the routes, create a backup of each cube. We don't want to delete or miss old data if we haven't validated the new data. As no dimension tables (except for `Geography`) need to be updated each time, we will try to re-ingest all historical data in order to keep candidate names and other dimensions with the latest definitions.

Fourthly, run the ETL's and make modifications in order to keep the process clean (no errors or inconsistencies). We aim to keep the cube's structure. It's good practice to change the name of the cube in the ETL to not touch the old cube.

Fifthly, validate the new structure. This means, compare with the old data (create some `fancy` SQL queries) so we don't loose information or add disorganized data.

Last but not least, clean cache and celebrate !

# Note: To run the pipelines do:

As we are ingesting all historical data, and we actually don't have a filter in the ETL's processes to update just one year, for now `year` param is just an ornament.

1. `election_house`: `bamboo-cli --folder Bamboo-files --entry election_house --year=<year> --force=True --output-db=<output-db>`
2. `election_president_county`: `bamboo-cli --folder Bamboo-files --entry election_president_county --force=True --alaska-table=False --output-db=<output-db>`
3. `election_president_state`: `bamboo-cli --folder Bamboo-files --entry election_president_state --year=<year> --force=True --output-db=<output-db>`
4. `election_senate`: `bamboo-cli --folder Bamboo-files --entry election_senate --year=<year> --force=True --ingest=True --output-db=<output-db>`
5. `electoral_college`: `Unknown yet`
6. `house_compact`: `bamboo-cli --folder Bamboo-files --entry house_compact --year=<year> --force=True --output-db=<output-db>`