# A simple example of how to use the openFEC wrapper
from openfec_wrapper import CandidateData

# Create CandidateData objects for senate, house, and president
p_candidates = CandidateData.presidential_candidates()
s_candidates = CandidateData.senate_candidates()

# Commented out house data as it is a large download that may result in a
# "429 Too many requests" API Error. This is handled by using an
# "upgraded key" from the FEC. Email apiinfo@fec.gov to find out about
# getting an upgraded key.
# h_candidates = CandidateData.house_candidates()

# Retrieve and parse JSON data from FEC. Convert to pandas dataframes.
president = p_candidates.dataframe()
senate = s_candidates.dataframe()
# house = h_candidates.dataframe()

print(president.head(20))
print(senate.head(20))
# print(house.head(20))
