import collections
import os
import sys
import pandas as pd
import nlp_method as nm
import numpy as np
import string
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector
from shared_steps import ExtractFECStep


class TransformStep(PipelineStep):

    # method for expnading the year
    @staticmethod
    def expand_year(df):
        candidate_list = []
        for index, row in df.iterrows():
            name, party, state, district, year_list, candidate_id = row.values
            for year in year_list:
                candidate_list.append([name, party, state, district, int(year), candidate_id])
        return candidate_list

    def run_step(self, prev_result, params):
        house, house_candidate = prev_result
        # transformation script removing null values and formating the data
        house = pd.read_csv(house, delimiter="\t")
        house['district'] = "50000US" + house.state_fips.astype(str).str.zfill(2) + house.district.astype(str).str.zfill(2)
        house["office"] = "House"
        house.loc[(house['runoff'].isnull()), 'runoff'] = False
        house.loc[((house['candidate'].isnull()) | (house["candidate"] == "other")), 'candidate'] = 'Other'
        house.loc[(house['party'].isnull()), 'party'] = 'Other'
        unavailable_name_list = ["Blank Vote/Scattering", "Blank Vote/Void Vote/Scattering", "Blank Votes", "blank vote", "Scatter", "Scattering", "scatter", "Void Vote", "Over Vote", "None Of The Above", "None Of These Candidates", "Not Designated", "Blank Vote/Scattering/ Void Vote", "Void Vote"]
        house.loc[(house.candidate.isin(unavailable_name_list)), 'party'] = "Unavailable"
        house.loc[(house.candidate.isin(unavailable_name_list)), 'candidate'] = "Blank Vote"
        house.loc[(house["candidate"] == "no name"), 'candidate'] = "Unavailable"
        house.loc[(house['stage'].isnull()), 'stage'] = 'gen'
        house.loc[(house['stage'] == 'gen'), 'stage'] = 'General'
        house.loc[(house['stage'] == 'pri'), 'stage'] = 'Primary'
        house['party'] = house['party'].apply(lambda x: string.capwords(x))
        house.loc[(house['party'] == "Democrat"), 'party'] = "Democratic"
        house.rename(columns={'state': 'geo_name', 'district': 'geo_id'}, inplace=True)

        # importing the FEC data
        house_candidate1 = house_candidate.loc[:, ["name", "party_full", "state", "district", "election_years", "candidate_id"]]
        house_candidate1 = pd.DataFrame(self.expand_year(house_candidate1))
        house_candidate1.columns = ["name", "party", "state", "district", "year", "candidate_id"]
        final_compare = nm.nlp_dict(house, house_candidate1, 2, False)  # getting the dictionary of the candidates names in MIT data and there match
        # below is the use of merge_insigni techniques to find out of the found blank strings which one is insignificant
        merge = nm.merge_insig(final_compare, house)
        nm.logging_helper(final_compare, merge)
        # creating a dictionary for the candidate name in fec data in the format of modified names as key and id as it's value
        house_Id_dict = collections.defaultdict(str)
        for candidate in house_candidate['name'].values:
            temp_id = house_candidate.loc[(house_candidate['name'] == candidate), 'candidate_id'].values
            house_Id_dict[nm.modify_fecname(candidate, True).lower()] = temp_id[0]
        # creating a list of candidate id for the candidates in the MIT data
        id_list = []
        for candidate in house['candidate'].values:
            temp = nm.formatname_mitname(candidate).replace('.', '').replace('"', '').replace('?', '\'').replace('_', ' ').lower()
            if temp in ['blank vote', 'other', 'unavailable']:
                id_list.append("H99999999")
            elif temp in final_compare:
                if final_compare[temp] == "":
                    id_list.append("H99999999")
                else:
                    id_list.append(house_Id_dict[final_compare[temp]])
            else:
                id_list.append("H99999999")
        house["candidate_id"] = id_list
        # using the normalizing name method to normalize the name and make them in the same format
        normalizedname_dict = collections.defaultdict(str)
        for index, row in house_candidate1.iterrows():
            name = row['name']
            cid = row['candidate_id']
            if cid in normalizedname_dict:
                continue
            normalizedname_dict[cid] = nm.normalize_name(name)
        # replacing the Normalized name from FEC data in MIT data
        candidate_l = []
        for index, row in house.iterrows():
            cid = row['candidate_id']
            name = row['candidate']
            if cid == "H99999999" and name in ['Blank Vote', 'Other', 'Unavailable']:
                candidate_l.append(name)
                continue
            elif cid == "H99999999":
                if nm.formatname_mitname(name).replace('.', '').replace('"', '').replace('?', '\'').replace('_', ' ').lower() in merge[0]:
                    candidate_l.append(nm.formatname_mitname(name).title())
                else:
                    candidate_l.append("Other")
            else:
                candidate_l.append(normalizedname_dict[cid])
        house['candidate'] = candidate_l
        house['candidate_other'] = house['candidate']
        # final transformation steps
        house.loc[(house['candidate_id'] == "H99999999"), 'candidate'] = "Other"
        house.drop(["state_cen", "state_ic", "state_po", "state_fips", "mode", "writein"], axis=1, inplace=True)
        house['special'] = house['special'].astype(np.int64)
        house['runoff'] = house['runoff'].astype(np.int64)
        house['unofficial'] = house['unofficial'].astype(np.int64)
        house['year'] = house['year'].astype(int)
        house['geo_name'] = house['geo_name'].apply(lambda x: string.capwords(x))

        house = house.drop("fusion_ticket", axis=1)

        # specific to create the compact table. Before was normal housing procedure
        house_compact = house[["year", "geo_id", "candidatevotes", "totalvotes", "candidate", "special", "party", "runoff"]].copy()
        house_compact.columns = ["year", "district", "winner_votes", "total_votes", "winning_candidate", "special", "party", "runoff"]

        house_compact = house_compact[house_compact["year"] >= 2008].reset_index(drop=True).copy()
        house_compact["other_votes"] = (house_compact["total_votes"] - house_compact["winner_votes"]).astype(int)

        house_compact = house_compact[["year", "district", "winner_votes", "other_votes", "total_votes", "winning_candidate", "special", "party", "runoff"]].copy()

        house_compact = house_compact.sort_values(["winner_votes"], ascending=False)
        house_compact.drop_duplicates(["year", "district"], keep="first", inplace=True)
        house_compact.reset_index(drop=True, inplace=True)
        house_compact = house_compact.sort_values(["year", "district"])
        house_compact.reset_index(drop=True, inplace=True)

        return house_compact


class ElectionHousePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("year", dtype=int),
            Parameter("force", dtype=bool),
            Parameter(label="Output database connector", name="output-db", dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):

        dtype = {
            "year": "INTEGER",
            "district": "varchar(255)",
            "winner_votes": "INTEGER",
            "other_votes": "INTEGER",
            "total_votes": "INTEGER",
            "winning_candidate": "varchar(255)",
            "special": "INTEGER",
            "party": "varchar(255)",
            "runoff": "INTEGER"
        }

        sys.path.append(os.getcwd())
        dl_step = DownloadStep(connector="ush-data", connector_path=__file__, force=params.get("force", False))
        fec_step = ExtractFECStep(ExtractFECStep.HOUSE)
        xform_step = TransformStep()
        load_step = LoadStep("election_house_compact", connector=params["output-db"], connector_path=__file__, dtype=dtype, if_exists="drop", pk=['year', 'district'], engine="ReplacingMergeTree", engine_params="version", schema="election")
        return [dl_step, fec_step, xform_step, load_step]