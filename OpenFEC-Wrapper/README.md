# OpenFEC_Wrapper
A Python wrapper for the OpenFEC API. Documentation for this API can be found [here](https://api.open.fec.gov/developers)

## Installation

 1. Clone this repository:
```
git clone https://github.com/Datawheel/DataUSA-Election/OpenFEC_Wrapper.git
```

 2. navigate into the new directory:
```
cd OpenFEC-Wrapper
```

 3. run command in cmd or terminal:
```
python setup.py install
```

### Setting environment variables on macOS
 4. Navigate to project root directory and set FEC_API_KEY environment variable with your actual API key:
```
export FEC_API_KEY=DEMO_KEY
```

### Setting environment variables on PC
 4. Navigate to project root directory and set FEC_API_KEY environment variable with your actual API key:
```
set FEC_API_KEY=DEMO_KEY
```
___
## Examples

### CANDIDATES

The `CandidateData` class holds information for Senate, House, and Presidential candidates.


#### presidential_candidates
The `CandidateData.presidential_candidates()` method is a factory method that retrieves all Presidential candidates from the FEC. This method sets the `CandidateData`'s `self._dataframe` object to a pandas dataframe of these candidates

```
from openfec_wrapper import CandidateData
p_candidates = CandidateData.presidential_candidates()
```

#### senate_candidates
The `CandidateData.senate_candidates()` method is a factory method that retrieves all Senate candidates from the FEC. This method sets the `CandidateData`'s `self._dataframe` object to a pandas dataframe of these candidates

```
from openfec_wrapper import CandidateData
s_candidates = CandidateData.senate_candidates()
```

#### house_candidates
The `CandidateData.house_candidates()` method is a factory method that retrieves all House candidates from the FEC. This method sets the `CandidateData`'s `self._dataframe` object to a pandas dataframe of these candidates

```
from openfec_wrapper import CandidateData
h_candidates = CandidateData.house_candidates()
```

##### dataframe
The `CandidateData.dataframe()` method will return a `pandas` dataframe object of all candidates for the specific `CandidateData(object)` class.

```
president = p_candidates.dataframe()
```
