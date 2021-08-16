import pandas as pd
import collections
import os
import sys
import string
from datetime import datetime
import nlp_method as nm
from shared_steps import ExtractFECStep
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector


class TransformStep(PipelineStep):

    # method for expnading the year
    @staticmethod
    def expand_year(df):
        candidate_list = []
        for index, row in df.iterrows():
            name, party, year_list, candidate_id = row.values
            for year in year_list:
                candidate_list.append([name, party, int(year), candidate_id])
        return candidate_list

    def run_step(self, prev_result, params):
        president, president_candidate = prev_result
        # transformation script removing null values and formating the data
        president = pd.read_csv(president, delimiter="\t")
        president['state_fips'] = "04000US" + president.state_fips.astype(str).str.zfill(2)
        president["office"] = "President"
        president.loc[president["writein"] == "False", "writein"] = False
        president.loc[president["writein"] == "True", "writein"] = True
        president.loc[(president['writein'] == True), 'candidate'] = "Other"
        president.loc[(president['writein'] == True), 'party'] = "Other"
        president.loc[(president['party'] == "no party affiliation") & (president['candidate'].isnull()), 'candidate'] = "Other"
        president.loc[(president['party'] == "other") & (president['candidate'].isnull()), 'candidate'] = "Other"
        president.loc[(president['party'] == "other"), 'party'] = "Other"
        president.loc[(president['party'] == "unenrolled") & (president['candidate'].isnull()), 'candidate'] = "Other"
        president.loc[(president['candidate'].isnull()), 'candidate'] = "Unavailable"
        president.loc[(president['year'] == 2012) & (president['candidate'] == "Mitt, Romney"), 'candidate'] = "Romney, Mitt"
        president.loc[((president['party'].isnull()) & (president['candidate'] == "Other")), 'party'] = "Other"
        unavailable_name_list = ["Blank Vote/Scattering", "Blank Vote/Void Vote/Scattering", "Blank Vote", "blank vote", "Scatter", "Scattering", "scatter", "Void Vote", "Over Vote", "None Of The Above", "None Of These Candidates", "Not Designated", "Blank Vote/Scattering/ Void Vote", "Void Vote"]
        president.loc[(president.candidate.isin(unavailable_name_list)), 'party'] = "Unavailable"
        president.loc[(president.candidate.isin(unavailable_name_list)), 'candidate'] = "Blank Vote"
        # improved capitalization
        president = president.rename(columns={"party": "party_ex", "party_detailed": "party"})
        president.loc[president["party"].isnull(), "party"] = "Other"
        president['party'] = president['party'].apply(lambda x: string.capwords(x))
        president.loc[(president['party'] == "Democrat"), 'party'] = "Democratic"
        president.drop(["notes", "state_cen", "state_ic", "state_po", "writein"], axis=1, inplace=True)
        president.rename(columns={'state': 'geo_name', 'state_fips': 'geo_id'}, inplace=True)
        president['geo_name'] = president['geo_name'].apply(lambda x: string.capwords(x))

        # importing the FEC data
        president_candidate1 = president_candidate.loc[:, ['name', 'party_full', 'election_years', 'candidate_id']]
        president_candidate1 = pd.DataFrame(self.expand_year(president_candidate1))
        president_candidate1.columns = ["name", "party", "year", "candidate_id"]

        final_compare = nm.nlp_dict(president, president_candidate1, 4, True)  # getting the dictionary of the candidates names in MIT data and there match
        # below is the use of merge_insigni techniques to find out of the found blank strings which one is insignificant
        merge = nm.merge_insig(final_compare, president)
        nm.logging_helper(final_compare, merge)
        # creating a dictionary for the candidate name in the modified names as key and values as it's id
        president_Id_dict = collections.defaultdict(str)
        for candidate in president_candidate['name'].values:
            temp_id = president_candidate.loc[(president_candidate['name'] == candidate), 'candidate_id'].values
            president_Id_dict[nm.modify_fecname(candidate, False).lower()] = temp_id[0]
        # creating a list of candidate id for the candidates in the MIT data
        id_list = []
        for candidate in president['candidate'].values:
            temp = candidate.replace('\\', '').replace('"', '').replace(',', '').lower()
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
        # using the normalizing name method to normalize the name and make them in the same format
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
            if cid == "P99999999" and name in ['Blank Vote', 'Other', 'Unavailable']:
                candidate_l.append(name)
                continue
            elif cid == "P99999999":
                sl = name.strip('\"').split(',')
                if len(sl) == 1:
                    candidate_l.append(sl[0])
                else:
                    temp1 = sl[0].strip().strip('\"').split(' ')
                    temp2 = sl[1].strip().strip('\"').split(' ')
                    candidate_l.append(temp2[0] + ' ' + temp1[0])
            else:
                candidate_l.append(normalizedname_dict[cid])
        president['candidate'] = candidate_l

        # final transformation steps
        president.loc[(president['candidate_id'] == "P99999999"), 'candidate'] = "Other"
        # fec_mit_result = pd.DataFrame(list(final_compare.items()), columns=["MIT data", "FEC data"])
        # fec_mit_result.to_csv("MIT_fec_president_NLTK_fuzzywuzzy.csv", index=False)
        president = president.groupby(["year", "geo_name", "geo_id", "office", "candidate", "party", "version", "candidate_id"]).agg({
            "candidatevotes": "sum",
            "totalvotes": "max"
        }).reset_index()

        # Tag on national data:
        president_state = president.copy()
        president_nation = president.copy()
        president_nation = president_nation.groupby(
            ["year", "office", "candidate",
            "party", "version", "candidate_id"]
        ).agg({
            "candidatevotes": "sum"
        }).reset_index()
        president_nation['geo_id'] = '01000US'
        president_nation['geo_name'] = 'United States'

        compressed_votes = president_nation.groupby("year").agg({
            "candidatevotes": "sum"
        }).reset_index()
        compressed_votes.rename(columns={"candidatevotes": "totalvotes"}, inplace=True)
        president_nation = president_nation.merge(compressed_votes, on=["year"], how="left")
        
        df = pd.concat([president_state, president_nation])

        if params["output-db"] == "clickhouse-database":
            df['version'] = datetime.now()
            df['version'] = pd.to_datetime(df['version'], infer_datetime_format=True)

        return df


class EelectionPresidentStatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("year", dtype=int),
            Parameter("force", dtype=bool),
            Parameter(label="Output database connector", name="output-db", dtype=str, source=Connector)
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
        dl_step = DownloadStep(connector="usp-data", connector_path=__file__, force=params.get("force", False))
        fec_step = ExtractFECStep(ExtractFECStep.PRESIDENT)
        xform_step = TransformStep()
        
        load_step = LoadStep("election_president", dtype={"geo_id": "varchar(255)", "party": "varchar(255)", "candidate_id": "varchar(255)"}, connector=params["output-db"], connector_path=__file__, if_exists="drop", pk=['year', 'candidate_id', 'party', 'geo_id'], engine="ReplacingMergeTree", engine_params="version", schema="election")
        load_step_click = LoadStep("election_president", dtype=dtype_click, connector=params["output-db"], connector_path=__file__, if_exists="drop", pk=['year', 'candidate_id', 'party', 'geo_id'], engine="ReplacingMergeTree")

        return [dl_step, fec_step, xform_step, load_step if params["output-db"] != "clickhouse-database" else load_step_click]
