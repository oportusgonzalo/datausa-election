import pandas as pd
import collections
import os
import sys
import nlp_method as nm
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector
from openfec_wrapper import CandidateData


class ExtractFECDataStep(PipelineStep):
    def run_step(self, prev_result, params):
        # Create CandidateData objects president
        p_candidates = CandidateData.presidential_candidates()
        president_fec = p_candidates.dataframe()
        return (prev_result, president_fec)


class TransformStep(PipelineStep):

    # method for expnading the year
    @staticmethod
    def expand_year(nd):
        candidate_list = []
        for index, row in nd.iterrows():
            row_list = row.values
            temp = row_list[2]
            # temp = temp.strip('{').strip('}').split(',')
            for year in temp:
                candidate_list.append([row_list[0], row_list[1], int(year), row_list[3]])
        return candidate_list

    @staticmethod
    def fips_code(codes_list):
        fips_list = []
        for code in codes_list:
            if code < 10:
                fips_list.append("04000US0"+str(code))
            else:
                fips_list.append("04000US"+str(code))
        return fips_list

    def run_step(self, prev_result, params):
        df = prev_result[0]
        # transformation script removing null values and formating the data
        president = pd.read_csv(df, delimiter="\t")
        president.drop(["notes", "state_cen", "state_ic"], axis=1, inplace=True)
        president['state_fips'] = self.fips_code(president['state_fips'])
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
        president.loc[(president['year'] == 2012) & (president['candidate'] == "Mitt, Romney")] = "Romney, Mitt"
        president.loc[((president['party'].isnull()) & (president['candidate'] == "Other")), 'party'] = "Other"
        president.loc[((president['candidate'] == "Blank Vote") | (president['candidate'] == "Blank Vote/Scattering") | (president['candidate'] == "Blank Vote/Scattering/ Void Vote") | (president['candidate'] == "Blank Vote/Void Vote/Scattering") | (president['candidate'] == "Scattering") | (president['candidate'] == "Void Vote") | (president['candidate'] == "Over Vote") | (president['candidate'] == "None Of The Above") | (president['candidate'] == "None Of These Candidates") | (president['candidate'] == "Not Designated")), 'party'] = "Unavailable"
        president.loc[((president['candidate'] == "Blank Vote") | (president['candidate'] == "Blank Vote/Scattering") | (president['candidate'] == "Blank Vote/Scattering/ Void Vote") | (president['candidate'] == "Blank Vote/Void Vote/Scattering") | (president['candidate'] == "Scattering") | (president['candidate'] == "Void Vote") | (president['candidate'] == "Over Vote") | (president['candidate'] == "None Of The Above") | (president['candidate'] == "None Of These Candidates") | (president['candidate'] == "Not Designated")), 'candidate'] = "Blank Vote"
        president['party'] = president['party'].str.title()

        # importing the FEC data
        # URL for the fec data
        # url = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3.us-gov-west-1.amazonaws.com/c7da9e486a631e8ba0766e118b658c5ecbab4e9d55754288eaa4c6b9.csv?response-content-disposition=filename%3Dcandidates-2019-07-22T10%3A07%3A11.csv&AWSAccessKeyId=AKIAR7FXZINYKQPW5N4V&Signature=uaaiCC4q6ngP0WXBo7GI%2BmFO9k4%3D&Expires=1564409232"
        # president_candidate = pd.read_csv(url)  # this url step will be replaced once we start using the custom ExtractFECdata step
        president_candidate = prev_result[1]
        president_candidate1 = president_candidate.loc[:, ['name', 'party_full', 'election_years', 'candidate_id']]
        president_candidate1 = pd.DataFrame(self.expand_year(president_candidate1))
        president_candidate1.columns = ["name", "party", "year", "candidate_id"]
        # president_candidate1.head()

        final_compare = nm.nlp_dict(president, president_candidate1, 4, True)  # getting the dictionary of the candidates names in MIT data and there match
        # below is the use of merge_insigni techniques to find out of the found blank strings which one is insignificant
        # merge = nm.merge_insig(final_compare, president)
        # print(len(merge[0]))
        # creating a dictionary for the candidate name and it's doictionary
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
        for index, row in president.iterrows():
            cid = row['candidate_id']
            name = row['candidate']
            if cid == "P99999999" and name in ['Blank Vote', 'Other', 'Unavailable']:
                continue
            elif cid == "P99999999":
                sl = name.strip('\"').split(',')
                if len(sl) == 1:
                    row['candidate'] = sl[0]
                else:
                    temp1 = sl[0].strip().strip('\"').split(' ')
                    temp2 = sl[1].strip().strip('\"').split(' ')
                    row['candidate'] = temp2[0]+' '+temp1[0]
            else:
                row['candidate'] = normalizedname_dict[cid]

        # final transformation steps
        president.loc[(president['candidate_id'] == "P99999999"), 'candidate'] = "Other"
        # fec_mit_result = pd.DataFrame(list(final_compare.items()), columns=["MIT data", "FEC data"])
        # fec_mit_result.to_csv("MIT_fec_president_NLTK_fuzzywuzzy.csv", index=False)
        # president.to_csv("President_election_1976-2016.csv", index=False)
        return president


class ExamplePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("year", dtype=int),
            Parameter("force", dtype=bool),
            Parameter(label="Output database connector", name="output-db", dtype=str, source=Connector),
            Parameter("folder", dtype=str)
        ]

    @staticmethod
    def steps(params):
        sys.path.append(os.getcwd())
        dl_step = DownloadStep(connector="usp-data", connector_path=__file__, force=params.get("force", False))
        fec_step = ExtractFECDataStep()
        xform_step = TransformStep()
        load_step = LoadStep("bls_test_import", connector=params["output-db"], connector_path=__file__,  if_exists="append")
        return [dl_step, fec_step, xform_step, load_step]
