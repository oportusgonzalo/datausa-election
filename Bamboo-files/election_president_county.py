import pandas as pd
import collections
import nlp_method as nm
import numpy as np
import sys
import os
import string
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
        president, president_candidate = prev_result
        # transformation script removing null values and formating the data
        president = pd.read_csv(president, delimiter="\t")

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
            president.loc[(president['FIPS'].str.contains("05000US027")),
                          'FIPS'] = '99999AK' + \
                president['FIPS'].str.slice(start=6)
        # Above logic incorrect, throw exception!
        except Exception:
            null_county = president.loc[
                president['FIPS'].isnull()].iloc[0]['county']
            raise Exception(
                'There exists null FIPS codes in the database. ' +
                'The county is: ' + str(null_county))

        # Hardcoded Oglala County SD fips fix
        president.loc[(president['FIPS'] == '05000US46113'),
                      'FIPS'] = '05000US46102'

        # Fix null party values
        president.loc[(president['party'].isnull()), 'party'] = 'Other'

        # Title case parties
        president['party'] = president['party'].apply(lambda x: string.capwords(x))
        president.loc[(president['party'] == "Democrat"), 'party'] = "Democratic"
        
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
        candidate_l = []
        for index, row in president.iterrows():
            cid = row['candidate_id']
            name = row['candidate']
            if cid == "P99999999" and name in ['Blank Vote', 'Other',
                                               'Unavailable']:
                candidate_l.append(name)
                continue
            elif cid == "P99999999":
                candidate_l.append(nm.formatname_mitname(
                    candidate).replace('.', '').lower())
            else:
                candidate_l.append(normalizedname_dict[cid])
        president['candidate'] = candidate_l

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


class AlaskaHouseStep():
    def run_step(self, prev_result, params):
        # Seperate Alaskan house districts into their own dimension table
        president = prev_result
        ak_house_dist = president.loc[(
            president['geo_id'].str.contains('99999AK')),
            ['geo_id', 'geo_name']]
        ak_house_dist = ak_house_dist.drop_duplicates()
        return ak_house_dist


class ElectionPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("force", dtype=bool),
            Parameter("alaska-table", dtype=bool),
            Parameter(label="Output database connector",
                      name="output-db", dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):

        dtype_click = {
            "year": "UInt16",
            "geo_name": "String",
            "geo_id": "String",
            "office": "String",
            "candidate": "String",
            "party": "String",
            "candidatevotes": "UInt32",
            "totalvotes": "UInt32",
            "version": "DateTime",
            "candidate_id": "String"
        }

        sys.path.append(os.getcwd())
        dl_step = DownloadStep(
            connector="uspc-data", connector_path=__file__,
            force=params.get("force", False))
        fec_step = ExtractFECStep(ExtractFECStep.PRESIDENT)
        xform_step = TransformStep()

        load_step = LoadStep(
            "election_president", connector=params["output-db"],
            connector_path=__file__, if_exists="append",
            pk=['year', 'candidate_id', 'party'],
            engine="ReplacingMergeTree", engine_params="version", schema="election")

        load_step_click = LoadStep(
            "election_president", connector=params["output-db"],
            connector_path=__file__, if_exists="append", dtype=dtype_click,
            pk=['year', 'candidate_id', 'party'],
            engine="ReplacingMergeTree", engine_params="version")
        
        ak_house_step = AlaskaHouseStep()
        ak_house_load_step = LoadStep(
            "alaska_housedist", connector=params["output-db"],
            connector_path=__file__, if_exists="append",
            pk=['geo_id', 'geo_name'])
        if params.get("alaska-table", False):
            return [dl_step, fec_step, xform_step, load_step if params["output-db"] != "clickhouse-database" else load_step_click, 
                ak_house_step, ak_house_load_step]
        else:
            return [dl_step, fec_step, xform_step, load_step if params["output-db"] != "clickhouse-database" else load_step_click]
