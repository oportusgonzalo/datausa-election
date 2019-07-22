# openfec_wrapper/fec.py
import time
import pandas as pd
from bamboo_lib.logger import logger
from pandas.io.json import json_normalize
from .utils import session


class CandidateData(object):
    def __init__(self, candidate_code):
        self._dataframe = CandidateData.retrieve_candidates(candidate_code)

    # Returns information on all candidates that ran for a specific office
    @staticmethod
    def retrieve_candidates(office):
        path = 'https://api.open.fec.gov/v1/candidates/?office={}'.format(
            office)
        response = session.get(
            path, headers=session.headers, params=session.params)
        data = response.json()['results']
        results = json_normalize(data)
        return candidate_recur(2, results, session.params,
                               session.headers, office)

    def dataframe(self):
        return self._dataframe

    @staticmethod
    def presidential_candidates():
        return CandidateData('P')

    @staticmethod
    def senate_candidates():
        return CandidateData('S')

    @staticmethod
    def house_candidates():
        return CandidateData('H')


# Recursive helper function for candidates method
def candidate_recur(page, df, params, headers, office):
    path = 'https://api.open.fec.gov/v1/candidates/?office={}'.format(
        office)
    params['page'] = str(page)
    response = session.get(path, headers=headers, params=params)
    data = response.json()['results']
    results = json_normalize(data)
    if page % 10 == 0 and page > 9:
        logger.info("Finished downloading pages: " +
                    str(page - 10) + " through " + str(page))
    if not results.empty:
        frames = [df, results]
        df = pd.concat(frames)
        time.sleep(1.0)
        return candidate_recur(page + 1, df, params, headers, office)
    else:
        return df
