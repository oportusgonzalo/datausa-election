# OpenFEC_Wrapper
A Python wrapper for the OpenFEC API. Documentation for this API can be found [here](https://api.open.fec.gov/developers)

## Installation

 1. Clone this repository:
```
git clone https://github.com/Datawheel/DataUSA-Election/OpenFEC_Wrapper.git
```

 2. navigate into the new directory:
```
cd fecwrapper
```

 3. run command in cmd or terminal:
```
python setup.py install
```

 4. Navigate to project root directory and set FEC_API_KEY environment variable with your actual API key:
 #### Setting environment variables on PC
```
set FEC_API_KEY=DEMO_KEY
```
#### Setting environment variables on macOS
```
export FEC_API_KEY=DEMO_KEY
```

## Examples

### CANDIDATES

The `CANDIDATES` class holds information for Senate, House, and Presidential candidates. To create these three classes, pass 'S', 'H', or 'P' to create `CANDIDATE` objects for Senate, House, and President, respectively.

Currently one method is available.

##### dataframe
The `CANDIDATES.dataframe()` method will return a `pandas` dataframe object of all candidates for the specific `CANDIDATE(office)` class.
