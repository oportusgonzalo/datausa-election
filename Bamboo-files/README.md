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
