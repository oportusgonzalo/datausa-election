# A simple example of how to use the openFEC wrapper
from openfec_wrapper import CANDIDATES

# Create CANDIDATE objects for senate, house, and president
presidential_candidates = CANDIDATES('P')
senate_candidates = CANDIDATES('S')
house_candidates = CANDIDATES('H')


# Retrieve and parse JSON data from FEC. Convert to pandas dataframes.
president = presidential_candidates.dataframe()
senate = senate_candidates.dataframe()

# Commented out house data as it is a large download that may result in a
# "429 Too many requests" API Error.
# house = house_candidates.dataframe()

print(president.head())
