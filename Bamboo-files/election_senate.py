import collections
import sys
import os
import pandas as pd
import numpy as np
import string
import nlp_method as nm
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector
from shared_steps import ExtractFECStep


class ManualFixStep(PipelineStep):
    def run_step(self, senate_df, params):
        # Fix for Angus King in Maine in 2018
        conds = (senate_df.geo_id == '04000US23') & (senate_df.year == 2018) & (senate_df.candidate_id == 'S99999999') & (senate_df.candidatevotes == 344575)
        assert len(senate_df[conds]) == 1
        senate_df.loc[conds, 'candidate'] = 'Angus S. King Jr.'
        senate_df.loc[conds, 'candidate_id'] = 'S2ME00109'

        # Fix for Bob Casey PA, 2006
        conds_casey_1 = (senate_df.geo_id == '04000US42') & (senate_df.year == 2006) & (senate_df.candidate_id == 'S99999999') & (senate_df.candidatevotes == 2392984)
        assert len(senate_df[conds_casey_1]) == 1
        senate_df.loc[conds_casey_1, 'candidate'] = 'Robert P. Casey Jr.'
        senate_df.loc[conds_casey_1, 'candidate_id'] = 'S6PA00217'

        # Fix for Bob Casey PA, 2012
        conds_casey_2 = (senate_df.geo_id == '04000US42') & (senate_df.year == 2012) & (senate_df.candidate_id == 'S99999999') & (senate_df.candidatevotes == 3021364)
        assert len(senate_df[conds_casey_2]) == 1
        senate_df.loc[conds_casey_2, 'candidate'] = 'Robert P. Casey Jr.'
        senate_df.loc[conds_casey_2, 'candidate_id'] = 'S6PA00217'
        return senate_df


class TransformStep(PipelineStep):

    # method for expnading the year
    @staticmethod
    def expand_year(df):
        candidate_list = []
        for index, row in df.iterrows():
            name, party, state, year_list, candidate_id = row.values
            for year in year_list:
                candidate_list.append([name, party, state, int(year), candidate_id])
        return candidate_list

    def run_step(self, prev_result, params):
        senate, senate_candidate = prev_result
        # transformation script removing null values and formating the data
        senate = pd.read_csv(senate, delimiter="\t")
        senate['state_fips'] = "04000US" + senate.state_fips.astype(str).str.zfill(2)
        senate["office"] = "Senate"
        senate.loc[(senate['candidate'].isnull()), 'candidate'] = 'Other'
        senate.loc[(senate['party'].isnull()), 'party'] = 'Other'
        senate['party'] = senate['party'].apply(lambda x: string.capwords(x))
        senate.loc[(senate['party'] == "Democrat"), 'party'] = "Democratic"
        unavailable_name_list = ["Blank Vote/Scattering", "Blank Vote/Void Vote/Scattering", "Blank Vote", "blank vote", "Scatter", "Scattering", "scatter", "Void Vote", "Over Vote", "None Of The Above", "None Of These Candidates", "Not Designated", "Blank Vote/Scattering/ Void Vote", "Void Vote"]
        senate.loc[(senate.candidate.isin(unavailable_name_list)), 'party'] = "Unavailable"
        senate.loc[(senate.candidate.isin(unavailable_name_list)), 'candidate'] = "Blank Vote"
        senate.loc[(senate['stage'] == "gen"), 'stage'] = "General"
        senate.loc[(senate['stage'] == "pre"), 'stage'] = "Primary"
        senate.rename(columns={'state': 'geo_name', 'state_fips': 'geo_id'}, inplace=True)
        senate['special'] = senate['special'].astype(np.int64)
        senate['unofficial'] = senate['unofficial'].astype(np.int64)
        senate['version'].fillna(99998899,inplace=True)
        senate['version'] = senate['version'].astype(np.int64)

        # importing the FEC data
        senate_candidate1 = senate_candidate.loc[:, ["name", "party_full", "state", "election_years", "candidate_id"]]
        senate_candidate1 = pd.DataFrame(self.expand_year(senate_candidate1))
        senate_candidate1.columns = ["name", "party", "state", "year", "candidate_id"]

        final_compare = nm.nlp_dict(senate, senate_candidate1, 2, False)  # getting the dictionary of the candidates names in MIT data and there match
        # below is the use of merge_insigni techniques to find out of the found blank strings which one is insignificant
        merge = nm.merge_insig(final_compare, senate)
        nm.logging_helper(final_compare, merge)
        # creating a dictionary for the candidate name and it's doictionary
        senate_Id_dict = collections.defaultdict(str)
        for candidate in senate_candidate['name'].values:
            temp_id = senate_candidate.loc[(senate_candidate['name'] == candidate), 'candidate_id'].values
            senate_Id_dict[nm.modify_fecname(candidate, True).lower()] = temp_id[0]
        # creating a list of candidate id for the candidates in the MIT data and also replacing the name of candidates having less than 5% of votes and naming them as Other
        id_list = []
        for candidate in senate['candidate'].values:
            temp = nm.formatname_mitname(candidate).replace('.', '').replace('"', '').replace('?', '\'').replace('_', ' ').lower()
            if temp in ['blank vote', 'other', 'unavailable']:
                id_list.append("S99999999")
            elif temp in final_compare:
                if final_compare[temp] == "":
                    id_list.append("S99999999")
                else:
                    id_list.append(senate_Id_dict[final_compare[temp]])
            else:
                id_list.append("S99999999")
        senate["candidate_id"] = id_list
        # using the normalizing name method to normalize the name and make them in the same format
        normalizedname_dict = collections.defaultdict(str)
        for index, row in senate_candidate1.iterrows():
            name = row['name']
            cid = row['candidate_id']
            if cid in normalizedname_dict:
                continue
            normalizedname_dict[cid] = nm.normalize_name(name)
        # replacing the Normalized name from FEC data in MIT data
        candidate_l = []
        for index, row in senate.iterrows():
            cid = row['candidate_id']
            name = row['candidate']
            if cid == "S99999999" and name in ['Blank Vote', 'Other', 'Unavailable']:
                candidate_l.append(name)
                continue
            elif cid == "S99999999":
                if nm.formatname_mitname(name).replace('.', '').replace('"', '').replace('?', '\'').replace('_', ' ').lower() in merge[0]:
                    candidate_l.append(nm.formatname_mitname(name).title())
                else:
                    candidate_l.append("Other")
            else:
                candidate_l.append(normalizedname_dict[cid])

        senate['candidate'] = candidate_l
        # Creating an additional column where if candidate_id = S99999999 the name will be replaced with Other and stored in candidate_other
        senate['candidate_other'] = senate['candidate']
        # final transformation steps
        senate.loc[(senate['candidate_id'] == "S99999999"), 'candidate_other'] = "Other"
        senate.drop(["state_cen", "state_ic", "mode", "state_po", "district", "writein"], axis=1, inplace=True)
        return senate


class ElectionSenatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("year", dtype=int),
            Parameter("force", dtype=bool),
            Parameter(label="Output database connector", name="output-db", dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        sys.path.append(os.getcwd())
        dl_step = DownloadStep(connector="ussenate-data", connector_path=__file__, force=params.get("force", False))
        fec_step = ExtractFECStep(ExtractFECStep.SENATE)
        xform_step = TransformStep()
        manual_fix_step = ManualFixStep()
        load_step = LoadStep("election_senate", schema="election", connector=params["output-db"], connector_path=__file__, if_exists="append", pk=['year', 'candidate_id', 'party'], engine="ReplacingMergeTree", engine_params="version")
        return [dl_step, fec_step, xform_step, manual_fix_step, load_step]
