import pandas as pd
import collections
import nlp_method as nm
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector


class TransformStep(PipelineStep):

    # method for expnading the year
    def expand_year(self, nd):
        candidate_list = []
        for index, row in nd.iterrows():
            i = row.values
            t = i[2]
            t = t.strip('{').strip('}').split(',')
            for x in t:
                candidate_list.append([i[0], i[1], int(x), i[3]])
        return candidate_list

    def fips_code(self, codes_list):
        fips_list = []
        for i in codes_list:
            if i < 10:
                fips_list.append("04000US0"+str(i))
            else:
                fips_list.append("04000US"+str(i))
        return fips_list

    def run_step(self, prev_result, params):
        df = prev_result
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

        # importing the FEC data
        # URL for the fec data
        url = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3.us-gov-west-1.amazonaws.com/c7da9e486a631e8ba0766e118b658c5ecbab4e9d55754288eaa4c6b9.csv?response-content-disposition=filename%3Dcandidates-2019-07-19T09%3A42%3A17.csv&AWSAccessKeyId=AKIAR7FXZINYKQPW5N4V&Signature=dvx1um1AfXYZb6x7HWEAOhEOP7Q%3D&Expires=1564148543"
        president_candidate = pd.read_csv(url)  # this url step will be replaced once we start using the custom ExtractFECdata step
        # president_candidate = gen_fec()
        president_candidate1 = president_candidate.loc[:, ['name', 'party_full', 'election_years', 'candidate_id']]
        president_candidate1 = pd.DataFrame(self.expand_year(president_candidate1))
        president_candidate1.columns = ["name", "party", "year", "candidate_id"]
        # president_candidate1.head()

        final_compare = nm.nlp_dict(president, president_candidate1)  # getting the dictionary of the candidates names in MIT data and there match
        # below is the use of merge_insigni techniques to find out of the found blank strings which one is insignificant
        # merge = nm.merge_insig(final_compare,president)
        # print(merge)
        # creating a dictionary for the candidate name and it's doictionary
        president_Id_dict = collections.defaultdict(str)
        for i in president_candidate['name'].values:
            t = president_candidate.loc[(president_candidate['name'] == i), 'candidate_id'].values
            president_Id_dict[nm.modify(i).lower()] = t[0]
        # creating a list of candidate id for the candidates in the MIT data
        id_list = []
        for i in president['candidate'].values:
            t = i.replace('\\', '').replace('"', '').replace(',', '').lower()
            if t in ['blank vote', 'other', 'unavailable']:
                id_list.append("P99999999")
            elif t in final_compare:
                if final_compare[t] == "":
                    id_list.append("P99999999")
                else:
                    id_list.append(president_Id_dict[final_compare[t]])
            else:
                id_list.append("P99999999")
        president["candidate_id"] = id_list
        # using the normalizing name method to normalize the name and make them in the same format
        normalizedname_dict = collections.defaultdict(str)
        for i, j in president_candidate1.iterrows():
            name = j['name']
            cid = j['candidate_id']
            if cid in normalizedname_dict:
                continue
            normalizedname_dict[cid] = nm.normalize_name(name)
        # replacing the Normalized name from FEC data in MIT data
        for i, j in president.iterrows():
            cid = j['candidate_id']
            name = j['candidate']
            if cid == "P99999999" and name in ['Blank Vote', 'Other', 'Unavailable']:
                continue
            elif cid == "P99999999":
                sl = name.strip('\"').split(',')
                if len(sl) == 1:
                    j['candidate'] = sl[0]
                else:
                    temp1 = sl[0].strip().strip('\"').split(' ')
                    temp2 = sl[1].strip().strip('\"').split(' ')
                    j['candidate'] = temp2[0]+' '+temp1[0]
            else:
                j['candidate'] = normalizedname_dict[cid]

        # final transformation steps
        president.loc[(president['candidate_id'] == "P99999999"), 'candidate'] = "Other"
        president['party'] = [i.title() for i in president['party'].values]
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
            Parameter(label="Output database connector", name="output-db", dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        dl_step = DownloadStep(connector="usp-data", connector_path=__file__, force=params.get("force", False))
        xform_step = TransformStep()
        load_step = LoadStep("bls_test_import", connector=params["output-db"], connector_path=__file__,  if_exists="append")
        return [dl_step, xform_step, load_step]
