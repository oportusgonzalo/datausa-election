import collections
import sys
import os
import pandas as pd
import nlp_method as nm
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector
from shared_steps import ExtractFECStep


class TransformStep(PipelineStep):

    # method for expnading the year
    @staticmethod
    def expand_year(nd):
        candidate_list = []
        for index, row in nd.iterrows():
            row_list = row.values
            temp = row_list[3]
            for year in temp:
                candidate_list.append([row_list[0], row_list[1], row_list[2], int(year), row_list[4]])
        return candidate_list

    def run_step(self, prev_result, params):
        df = prev_result[0]
        # transformation script removing null values and formating the data
        senate = pd.read_csv(df, delimiter="\t")
        senate['state_fips'] = "04000US" + senate.state_fips.astype(str).str.zfill(2)
        senate["office"] = "Senate"
        senate.loc[(senate['candidate'].isnull()), 'candidate'] = 'Other'
        senate.loc[(senate['party'].isnull()), 'party'] = 'Other'
        senate['party'] = senate['party'].str.title()
        senate.loc[(senate["writein"] == "False"), "writein"] = False
        senate.loc[(senate["writein"] == "True"), "writein"] = True
        senate.loc[((senate['candidate'] == "Blank Vote/Scattering") | (senate['candidate'] == "Blank Vote/Void Vote/Scattering") | (senate['candidate'] == "Blank Vote") | (senate['candidate'] == "blank vote") | (senate['candidate'] == "Scatter") | (senate['candidate'] == "scatter") | (senate['candidate'] == "Void Vote") | (senate['candidate'] == "Over Vote") | (senate['candidate'] == "None Of The Above") | (senate['candidate'] == "None Of These Candidates") | (senate['candidate'] == "Not Designated")), 'party'] = "Unavailable"
        senate.loc[((senate['candidate'] == "Blank Vote/Scattering") | (senate['candidate'] == "Blank Vote/Void Vote/Scattering") | (senate['candidate'] == "Blank Vote") | (senate['candidate'] == "blank vote") | (senate['candidate'] == "Scatter") | (senate['candidate'] == "scatter") | (senate['candidate'] == "Void Vote") | (senate['candidate'] == "Over Vote") | (senate['candidate'] == "None Of The Above") | (senate['candidate'] == "None Of These Candidates") | (senate['candidate'] == "Not Designated")), 'candidate'] = "Blank Vote"
        senate.rename(columns={'state': 'geo_name', 'state_fips': 'geo_id'}, inplace=True)
        senate.drop(["state_cen", "state_ic", "mode", "state_po", "district", "writein"], axis=1, inplace=True)

        # importing the FEC data
        senate_candidate = prev_result[1]
        senate_candidate1 = senate_candidate.loc[:, ["name", "party_full", "state", "election_years", "candidate_id"]]
        senate_candidate1 = pd.DataFrame(self.expand_year(senate_candidate1))
        senate_candidate1.columns = ["name", "party", "state", "year", "candidate_id"]

        final_compare = nm.nlp_dict(senate, senate_candidate1, 2, False)  # getting the dictionary of the candidates names in MIT data and there match
        # below is the use of merge_insigni techniques to find out of the found blank strings which one is insignificant
        merge = nm.merge_insig(final_compare, senate)
        print(len(merge[0]))
        print(merge[0])
        # print(nm.check(final_compare).head())
        # creating a dictionary for the candidate name and it's doictionary
        senate_Id_dict = collections.defaultdict(str)
        for candidate in senate_candidate['name'].values:
            temp_id = senate_candidate.loc[(senate_candidate['name'] == candidate), 'candidate_id'].values
            senate_Id_dict[nm.modify_fecname(candidate, True).lower()] = temp_id[0]
        # creating a list of candidate id for the candidates in the MIT data and also replacing the name of candidates having less than 5% of votes and naming them as Other
        id_list = []
        for candidate in senate['candidate'].values:
            temp = nm.formatname_mitname(candidate).replace('.', '').lower()
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
                if nm.formatname_mitname(name).replace('.', '').lower() in merge[0]:
                    candidate_l.append(nm.formatname_mitname(name).title())
                else:
                    candidate_l.append("Other")
            else:
                candidate_l.append(normalizedname_dict[cid])
        senate['candidate'] = candidate_l
        # final transformation steps
        # senate.loc[(senate['candidate_id'] == "S99999999"), 'candidate'] = "Other"
        # fec_mit_result = pd.DataFrame(list(final_compare.items()), columns=["MIT data", "FEC data"])
        # fec_mit_result.to_csv("MIT_fec_senate_NLTK_fuzzywuzzy.csv", index=False)
        # senate.to_csv("Senate_election_1976-2016.csv", index=False)
        return senate


class ExamplePipeline(EasyPipeline):
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
        load_step = LoadStep("Senate_Election", connector=params["output-db"], connector_path=__file__,  if_exists="append")
        return [dl_step, fec_step, xform_step, load_step]
