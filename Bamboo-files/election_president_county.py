import pandas as pd
import collections
import nlp_method as nm
import numpy as np
import sys
import os
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector
from shared_steps import ExtractFECStep


class TransformStep(PipelineStep):

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
                vote_list.append(newTotal)
            else:
                vote_list.append(totalvotes.at[row])
        return vote_list

    # method for expnading the year
    @staticmethod
    def expand_year(nd):
        candidate_list = []
        for index, row in nd.iterrows():
            row_list = row.values
            temp = row_list[2]
            for year in temp:
                candidate_list.append(
                    [row_list[0], row_list[1], int(year), row_list[3]])
        return candidate_list

    def run_step(self, prev_result, params):
        df = prev_result[0]
        # transformation script removing null values and formating the data
        president = pd.read_csv(df, delimiter="\t")

        # Fills in null state_po's
        null_state_po = {}
        for index, row in president.loc[
                president['state_po'].isnull()].iterrows():
            if not row['state'] in null_state_po:
                null_state_po.update({row['state']: president.loc[
                    (president['state_po'].notnull()) & (
                        president['state'] == row['state']),
                    'state_po'].iloc[0]})
                president.loc[(president['state'] == row['state']),
                              'state_po'] = null_state_po[row['state']]

        # Reformat counties
        county_list = []
        for index, row in president.iterrows():
            if np.isnan(row['FIPS']):
                county_list.append(row['county'] + ", " + row['state_po'])
            else:
                county_list.append(
                    row['county'] + " County, " + row['state_po'])
        president['county'] = county_list

        # Custom Fips codes assigned to be non repeating
        null_fips = {}
        for index, row in president.loc[president['FIPS'].isnull()].iterrows():
            if not row['county'] in null_fips:
                null_fips.update({row['county']: president.loc[
                    (president['FIPS'].notnull()) & (
                        president['state'] == row['state']), 'FIPS'].max()
                    + 1})
                president.loc[president['county'] ==
                              row['county'], 'FIPS'] = null_fips[row['county']]

        # Append FIP's code prefixes. Throw exception if null values exist
        try:
            president['FIPS'] = "05000US" + \
                president['FIPS'].astype(int).astype(str).str.zfill(5)
        # Above logic incorrect, throw exception!
        except Exception:
            null_county = president.loc[
                president['FIPS'].isnull()].iloc[0]['county']
            raise Exception(
                'There exists null FIPS codes in the database. ' +
                'The county is: ' + str(null_county))

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
        president.drop(['index', 'state_po', 'state'], axis=1, inplace=True)

        # Rename FIPS and county column
        president.rename(columns={'FIPS': 'geo_id'}, inplace=True)
        president.rename(columns={'county': 'geo_name'}, inplace=True)
        president['candidatevotes'] = president['candidatevotes'].astype(
            np.int64)
        president['totalvotes'] = president['totalvotes'].astype(np.int64)
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
        fec_step = ExtractFECStep(ExtractFECStep.PRESIDENT)
        xform_step = TransformStep()
        load_step = LoadStep(
            "president_election", connector=params["output-db"],
            connector_path=__file__,  if_exists="append", pk=['candidate_id'])
        return [dl_step, fec_step, xform_step, load_step]
