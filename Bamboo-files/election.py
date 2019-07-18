import pandas as pd
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import collections
import re
import requests
import nltk
from pandas.io.json import json_normalize

class TransformStep(PipelineStep):
    def transform(self,president):
        #URL for the fec data
        #url="https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3.us-gov-west-1.amazonaws.com/c7da9e486a631e8ba0766e118b658c5ecbab4e9d55754288eaa4c6b9.csv?response-content-disposition=filename%3Dcandidates-2019-07-15T09%3A39%3A57.csv&AWSAccessKeyId=AKIAR7FXZINYMI3G4BRV&Signature=lXJWKk4M2VFsPkN9j4SAzS%2FUCCw%3D&Expires=1563802805"
        ## method for inputing the FEC Data from API
        def gen_fec():
        #     logger.debug("Downloading Data")
            headers = {
                'accept': 'application/json',
            }

            response = requests.get(
                # To collect house/senate data, change the 'P' after 'office=' to a 'H' or 'S', respectively
                # Must also change the value on line 38 in params to 'H' or 'S' from 'P'
                'https://api.open.fec.gov/v1/candidates/?per_page=100&sort_nulls_last=false&sort=name&page=1&office=P&api_key=KdFRSrPvxP8hlc1pDGeDtyb7WZ87uXN2qhKvF822&sort_null_only=false&sort_hide_null=false', headers=headers)
            data = response.json()['results']
            results = json_normalize(data)
            return gen_fec_help(results, 2)

        # A helper method for gen_fec
        # df: the dataframe to be passed through each recurrence
        # page: the current page number
        def gen_fec_help(df, page):
            headers = {
                'accept': 'application/json',
            }

            params = (
                ('per_page', '100'),
                ('sort_nulls_last', 'false'),
                ('sort', 'name'),
                ('page', str(page)),
                ('office', 'P'),
                ('api_key', 'KdFRSrPvxP8hlc1pDGeDtyb7WZ87uXN2qhKvF822'),
                ('sort_null_only', 'false'),
                ('sort_hide_null', 'false'),
            )
            response = requests.get(
                'https://api.open.fec.gov/v1/candidates/', headers=headers, params=params)
            # NB. Original query string below. It seems impossible to parse and
            # reproduce query strings 100% accurately so the one below is given
            # in case the reproduced version is not "correct".
            # response = requests.get('https://api.open.fec.gov/v1/candidates/?per_page=100&sort_nulls_last=false&sort=name&page=1&office=P&api_key=KdFRSrPvxP8hlc1pDGeDtyb7WZ87uXN2qhKvF822&sort_null_only=false&sort_hide_null=false', headers=headers)

            data = response.json()['results']
            results = json_normalize(data)

            if not results.empty:
                frames = [df, results]
                df = pd.concat(frames)
                return gen_fec_help(df, page + 1)
            else:
        #         logger.info("Loaded all " + str(page) + " pages.")
                return df

        #method for inputing fips_code
        def fips_code(code_list):
            fips_list = []
            for code in code_list:
                if code <10:
                    fips_list.append("04000US0"+str(code))
                else:
                    fips_list.append("04000US"+str(code))
            return fips_list

        ### method for expnading the year
        def eyear(fecdataframe):
            candidate_list = []
            for index,row in fecdataframe.iterrows():
                i = row.values
                temp = i[2]
                for year in temp:
                    candidate_list.append([i[0],i[1],int(year),i[3]])
            return candidate_list

        ###Below code is for performing match operation of the nammes
        #Modifciation method
        def modify(name):
            sl = name.replace('"','').split(',')
            #sl.reverse()
            s = ""
            for i in sl:
                s = s+i+" "
            return s.strip()
        #subsequent string
        def subsequence(s1,s2,m,n):
            b = 0
            a = 0
            while b < m and a < n:
                if s1[b] == s2[a]:
                    b += 1
                a += 1
            return b == m


        #first pass on the data matching
        def fpass(fec_list_names,mit_list_names):
            #fec_list_names is a list of candidate from fec and mit_list_names is a list of candidates from mit
            #output will be a dictionary
            first_dict = collections.defaultdict(list)
            for candidate in mit_list_names:
                flag = 0
                for fec_name in x:
        # if i is equal to j we append it to the list of the hashmap for the name in the mit data
                    if candidate == fec_name:
                        first_dict[candidate].append(fec_name)
                        break
        # if they are similar we merge all the possible outcomes and use fuzz ratio technique to find the best match out of it
                    elif subsequence(candidate,fec_name),len(candidate),len(fec_name)) or subsequence(fec_name,candidate,len(fec_name),len(canidate)):
                        first_dict[candidate].append(fec_name)
                    else:
                        flag += 1
                # if it found nowhere in the fec data we add ""(blank string) to the dictionary
                if flag >= len(fec_list_names):
                    first_dict[candidate].append("")
            return first_dict


        # # final pass
        def out(fpass_dict,fec_list_names):
        # fpass_dict is the dictionary of the output from the fpass
            compare = []
            for key,value in fpass_dict.items():
        # if the length of the dictonary is 1 and contains a null value we use fuzzy wuzzy logic to extract all the possible outcomes.
        # After the possible matches we run the jakkard similarity test to get the closest possible and also confirms it's partial ratio 83% to avoid false postitives
                if len(value) == 1:
                    if value[0] == "":
                        possible_match = process.extract(key,x)
                        min_dist = 1.1
                        s = ""
                        for cvalues in possible_match:
                            dist = nltk.jaccard_distance(set(nltk.ngrams(nltk.word_tokenize(key),n=1)),set(nltk.ngrams(nltk.word_tokenize(cvalues[0]),n=1)))
                            if dist < min_dist and dist <= 0.75 and fuzz.partial_ratio(key,cvalues[0]) >= 83:
                                min_dist = dist
                                s = cvalues[0]
                        compare.append([key,s])
                    else:
                        compare.append([key,value[0]])
                else:
                    s = ""
                    m = -1
        # if there is already a potential match we just loook for the best from the posible match found using the previous logic
                    for i in value:
                        if fuzz.ratio(key,i) > m and fuzz.ratio(key,i) >= 86:
                            m = fuzz.ratio(key,i)
                            s = i
                    compare.append([key,s])
            return compare


            # creating the final dictionary
            def result(out_list):
                # out_list is the list of all the canidate match from the out method and converts it into dictionary
                # Key in the dictionary is modified MIT data and value is it's equivalent in modified name in fec data
                final_dict = collections.defaultdict(str)
                for matched in out_list:
                    if matched[0] not in final_dict or final_dict[matched[0]] == "":
                        final_dict[matched[0]] = matched[1]
                return final_dict

            # count the matched and unmatched output
            def check(c):
                m = 0
                um = 0
                nm = 0
                for i,j in c.items():
                    if i == j:
                        m += 1
                    elif j == "":
                        um += 1
                    else:
                        nm += 1
                return pd.DataFrame([m,um,nm],index=["Matched_fully","Blank_string","Matched_partially"])
            # Merges all insignificant candidates for my pleasure... (Done by the percentage of votes recieved and name matching)
            #C is the list of candidates
            #df is the main dataframe
            def merge_insig(d, df):
                keep = []
                other = []
                for candidate in d.keys():
                    if(d[candidate]==''):
                        kept = False
                        if candidate == 'other':
                            kept = True
                            other.append(candidate)
                        else:
                            location = df.loc[(df['candidate'].str.lower()==candidate)]
                            byyear = location.groupby('year')
                            cvote = byyear.sum()['candidatevotes']
                            tvote = byyear.sum()['totalvotes']
                            for year in cvote.index:
                                percentage = cvote[year]/tvote[year]*100
                                if percentage > 10:
                                    keep.append(candidate)
                                    kept = True
                                    break
                        if kept == False:
                            other.append(candidate)
                            df.loc[(df['candidate'].str.lower() == candidate),'candidate'] = 'Other'
                return (keep,other)
            ### Method for normalizing the name
            #Moves suffix to end of name
            def append_suff(x):
                for suf in ['Jr.','Sr.']:
                    sl = re.split(suf, x)
                    if len(sl) > 1:
                        seperator = ''
                        x = seperator.join(sl) + " " + suf
                return x
            #normalize the name
            def normalize_name(name):
                name = re.sub(r"MR.|DR.|MRS.|MS.|PROF.|PH.D." , "" , name)
                a = name.find('(')
                b = name.find(')')
                c = name.find('/')
                if a != -1 and b != -1:
                    name = name[:a]+name[b+1:]
                if c != -1:
                    name = name.split('/')[0]
                name_list = name.split(',')
                if len(name_list) == 1:
                    return name.title()
                ln = name_list[0]
                fn = ' '.join(name_list[1].split())
                return ' '.join(append_suff((fn + " " + ln).title()).split())

            #method to check if the


            ##transformation script removing null values and formating the data
            president.drop(["notes","state_cen","state_ic"],axis=1,inplace=True)
            president['state_fips']=fips_code(president['state_fips'])
            president["office"]="President"
            president.loc[president["writein"]=="False","writein"]=False
            president.loc[president["writein"]=="True","writein"]=True
            president.loc[(president['writein']==True),'candidate']="Other"
            president.loc[(president['writein']== rue),'party']="Other"
            president.loc[(president['party']=="no party affiliation")&(president['candidate'].isnull()),'candidate']="Other"
            president.loc[(president['party']=="other")&(president['candidate'].isnull()),'candidate']="Other"
            president.loc[(president['party']=="other"),'party']="Other"
            president.loc[(president['party']=="unenrolled")&(president['candidate'].isnull()),'candidate']="Other"
            president.loc[(president['candidate'].isnull()),'candidate']="Unavailable"
            president.loc[(president['year']==2012)&(president['candidate']=="Mitt, Romney")]="Romney, Mitt"
            president.loc[((president['party'].isnull())&(president['candidate']=="Other")),'party']="Other"
            president.loc[((president['candidate']=="Blank Vote")|(president['candidate']=="Blank Vote/Scattering")|(president['candidate']=="Blank Vote/Scattering/ Void Vote")|(president['candidate']=="Blank Vote/Void Vote/Scattering")|(president['candidate']=="Scattering")|(president['candidate']=="Void Vote")|(president['candidate']=="Over Vote")|(president['candidate']=="None Of The Above")|(president['candidate']=="None Of These Candidates")|(president['candidate']=="Not Designated")),'party']="Unavailable"
            president.loc[((president['candidate']=="Blank Vote")|(president['candidate']=="Blank Vote/Scattering")|(president['candidate']=="Blank Vote/Scattering/ Void Vote")|(president['candidate']=="Blank Vote/Void Vote/Scattering")|(president['candidate']=="Scattering")|(president['candidate']=="Void Vote")|(president['candidate']=="Over Vote")|(president['candidate']=="None Of The Above")|(president['candidate']=="None Of These Candidates")|(president['candidate']=="Not Designated")),'candidate']="Blank Vote"

            ### importing the FEC data
            #president_candidate=pd.read_csv(url)
            president_candidate = gen_fec()
            president_candidate1 = president_candidate.loc[:,['name','party_full','election_years','candidate_id']]
            president_candidate1 = pd.DataFrame(eyear(president_candidate1))
            president_candidate1.columns = ["name","party","year","candidate_id"]
            #president_candidate1.head()

            ####below steps perform the matching operations and generate the result accordingly
            final_l = []
            for year in range(1976,2020,4):
                x = [modify(i).lower() for i in president_candidate1.loc[(president_candidate1["year"]==year),"name"].unique()]
                y = [i.replace('\\','').replace('"','').replace(',','').lower() for i in president.loc[(president["year"]==year),"candidate"].unique()]
                z = fpass(x,y)
                print("fpass done",year)
                final_l=final_l+out(z,x)
                print("spass done",year)
            final_compare = result(final_l)
            ###below is the use of merge_insigni techniques to find out of the found blank strings which one is insignificant
            merge = merge_insig(final_compare,president)
            # print(merge)
            ##creating a dictionary for the candidate name and it's doictionary
            president_Id_dict = collections.defaultdict(str)
            for i in president_candidate['name'].values:
                t = president_candidate.loc[(president_candidate['name']==i),'candidate_id'].values
                president_Id_dict[modify(i).lower()] = t[0]
            ### creating a list of candidate id for the candidates in the MIT data
            id_list = []
            for i in president['candidate'].values:
                t = i.replace('\\','').replace('"','').replace(',','').lower()
                if t in ['blank vote','other','unavailable']:
                    id_list.append("P99999999")
                elif t in final_compare:
                    if final_compare[t] == "":
                        id_list.append("P99999999")
                    else:
                        id_list.append(president_Id_dict[final_compare[t]])
                else:
                    id_list.append("P99999999")
            president["candidate_id"] = id_list
            ### using the normalizing name method to normalize the name and make them in the same format
            normalizedname_dict = collections.defaultdict(str)
            for i,j in president_candidate1.iterrows():
                name = j['name']
                cid = j['candidate_id']
                if cid in normalizedname_dict:
                    continue
                normalizedname_dict[cid] = normalize_name(name)
            for i,j in president.iterrows():
                cid = j['candidate_id']
                name = j['candidate']
                if cid == "P99999999" and name in ['Blank Vote','Other','Unavailable']:
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

            ### final transformation steps
            president.loc[(president['candidate_id']=="P99999999"),'candidate'] = "Other"
            president['party'] = [i.title() for i in president['party'].values]

            final = pd.DataFrame(list(final_compare.items()),columns=["MIT data","FEC data"])
            return president


    def run_step(self, prev_result, params):
        df = prev_result
        df = pd.read_csv(df,delimiter="\t")
        return self.transform(df)

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
        my_connectors=["usp-data"]
        dl_step = DownloadStep(connector=my_connectors, connector_path=__file__, force=params.get("force", False))
        xform_step = TransformStep()
        load_step = LoadStep("bls_test_import", connector=params["output-db"], connector_path=__file__,  if_exists="append")
        return [dl_step, xform_step, load_step]
