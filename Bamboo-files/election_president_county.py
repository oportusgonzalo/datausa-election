import pandas as pd
import collections
import nlp_method as nm
import math
import numpy as np
import nltk
import sys
import os
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector
from openfec_wrapper import CandidateData
nltk.download('punkt')


class ExtractFECDataStep(PipelineStep):
    def run_step(self, prev_result, params):
        # Create CandidateData objects president
        p_candidates = CandidateData.presidential_candidates()
        president_fec = p_candidates.dataframe()
        return (prev_result, president_fec)


class TransformStep(PipelineStep):

    # This method removes the decimal from the FIPS code and appends the prefix
    # of county 05000US with padding of XXAAA.
    @staticmethod
    def fips_code(codes_list):
        fips_list = []
        for code in codes_list:
            if math.isnan(code):
                fips_list.append(str(code))
            elif code < 10000.0:
                code = str(int(code))
                fips_list.append("05000US0" + code)
            else:
                code = str(int(code))
                fips_list.append("05000US" + code)
        return fips_list

    # Calculates the total votes for a null totalvotes value using the
    # candidatevotes sum for equal FIPS code and year
    @staticmethod
    def calcTotal(df, totalvotes, fips, year):
        vote_list = []
        for row in range(0, totalvotes.size):
            # If total votes at index i is null
            if np.isnan(totalvotes.at[row]):
                # Calculate the new totalvotes value using the candidatevotes
                # at identical FIPS and year to null totalvotes
                newTotal = df[(df['FIPS'] == fips.at[row]) & (
                    df['year'] == year.at[row])].sum()['candidatevotes']
                str(vote_list.append(newTotal))
            else:
                str(vote_list.append(totalvotes.at[row]))
        return vote_list

    # method for expnading the year
    @staticmethod
    def expand_year(nd):
        candidate_list = []
        for index, row in nd.iterrows():
            row_list = row.values
            temp = row_list[2]
            # temp = temp.strip('{').strip('}').split(',')
            for year in temp:
                candidate_list.append(
                    [row_list[0], row_list[1], int(year), row_list[3]])
        return candidate_list

    def run_step(self, prev_result, params):
        df = prev_result[0]
        # transformation script removing null values and formating the data
        president = pd.read_csv(df, delimiter="\t")

        # Fix the FIPS codes using method defined above
        president['FIPS'] = self.fips_code(president['FIPS'])

        # Fix null party values
        president.loc[(president['party'].isnull()), 'party'] = 'Other'

        # Title case parties
        president['party'] = president['party'].str.title()

        # Removes null candidate votes as MIT has stated that this is just
        # the way counties reported their data.
        president = president[(president['candidatevotes'].notnull())]
        total = president.isnull().sum().sort_values(ascending=False)
        percent = (president.isnull().sum() /
                   president.isnull().count()).sort_values(ascending=False)
        pd.concat([total, percent], keys=["Total", "Percent"], axis=1)

        # Reset all indices after drops
        president = president.reset_index()

        # Fix null totalvotes using the above function
        president['totalvotes'] = self.calcTotal(
            president, president['totalvotes'], president['FIPS'],
            president['year'])

        # importing the FEC data
        president_candidate = prev_result[1]
        president_candidate1 = president_candidate.loc[:, [
            'name', 'party_full', 'election_years', 'candidate_id']]
        president_candidate1 = pd.DataFrame(
            self.expand_year(president_candidate1))
        president_candidate1.columns = [
            "name", "party", "year", "candidate_id"]

        # getting the dictionary of the candidates names in MIT data and
        # there match
        final_compare = nm.nlp_dict(president, president_candidate1, 4, False)

        # creating a dictionary for the candidate name and it's dictionary
        president_Id_dict = collections.defaultdict(str)
        for candidate in president_candidate['name'].values:
            temp_id = president_candidate.loc[(
                president_candidate['name'] == candidate),
                'candidate_id'].values
            president_Id_dict[nm.modify_fecname(
                candidate, True).lower()] = temp_id[0]

        # creating a list of candidate id for the candidates in the MIT data
        id_list = []
        for candidate in president['candidate'].values:
            temp = nm.formatname_mitname(candidate).replace('.', '').lower()
            if temp in ['blank vote', 'other', 'unavailable']:
                id_list.append("P99999999")
            elif temp in final_compare:
                if final_compare[temp] == "":
                    id_list.append("P99999999")
                else:
                    id_list.append(president_Id_dict[final_compare[temp]])
            else:
                id_list.append("P99999999")
        president["candidate_id"] = id_list

        # using the normalizing name method to normalize the name and
        # make them in the same format
        normalizedname_dict = collections.defaultdict(str)
        for index, row in president_candidate1.iterrows():
            name = row['name']
            cid = row['candidate_id']
            if cid in normalizedname_dict:
                continue
            normalizedname_dict[cid] = nm.normalize_name(name)

        # replacing the Normalized name from FEC data in MIT data
        for index, row in president.iterrows():
            cid = row['candidate_id']
            name = row['candidate']
            if cid == "P99999999" and name in ['Blank Vote', 'Other',
                                               'Unavailable']:
                continue
            elif cid == "P99999999":
                row['candidate'] = nm.formatname_mitname(
                    candidate).replace('.', '').lower()
            else:
                row['candidate'] = normalizedname_dict[cid]

        # final transformation steps
        president.loc[(president['candidate_id'] == "P99999999"),
                      'candidate'] = "Other"
        president.drop('state_po', axis=1, inplace=True)

        # Rename FIPS and county column
        president.rename(columns={'FIPS': 'geo_id'}, inplace=True)
        president.rename(columns={'county': 'geo_name'}, inplace=True)

        return president


class ExamplePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("year", dtype=int),
            Parameter("force", dtype=bool),
            Parameter(label="Output database connector",
                      name="output-db", dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        sys.path.append(os.getcwd())
        dl_step = DownloadStep(
            connector="uspc-data", connector_path=__file__,
            force=params.get("force", False))
        fec_step = ExtractFECDataStep()
        xform_step = TransformStep()
        load_step = LoadStep(
            "president_election_county", connector=params["output-db"],
            connector_path=__file__,  if_exists="append")
        return [dl_step, fec_step, xform_step, load_step]
