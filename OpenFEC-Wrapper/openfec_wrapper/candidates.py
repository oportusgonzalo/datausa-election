# openfec_wrapper/fec.py
import pandas as pd
from pandas.io.json import json_normalize
from .utils import session


class CANDIDATES(object):
    def __init__(self, office):
        self.office = office

    # Returns information on all candidates that ran for a specific office
    def dataframe(self):
        path = 'https://api.open.fec.gov/v1/candidates/?office={}'.format(
            self.office)
        response = session.get(
            path, headers=session.headers, params=session.params)
        data = response.json()['results']
        results = json_normalize(data)
        print("COMPLETED INITIAL DATA GRAB")
        return candidate_recur(self, 2, results, session.params, 
                               session.headers)


# Recursive helper function for candidates method
def candidate_recur(self, page, df, params, headers):
    print("RECURRENCE NUMB: " + str(page))
    path = 'https://api.open.fec.gov/v1/candidates/?office={}'.format(
        self.office)
    params['page'] = str(page)
    response = session.get(path, headers=headers, params=params)
    data = response.json()['results']
    results = json_normalize(data)

    if page % 10 == 0 & page > 9:
        print("Retrieved pages: " + str(page) + " through " + str(page - 10))
    if not results.empty:
        print("NEXT PAGE")
        frames = [df, results]
        df = pd.concat(frames)
        return candidate_recur(self, page + 1, df, params, headers)
    else:
        return df
