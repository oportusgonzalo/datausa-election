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
