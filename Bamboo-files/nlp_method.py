import pandas as pd
import collections
import re
import nltk
from fuzzywuzzy import process, fuzz


# Modification method
def modify(name):
    sl = name.replace('"', '').split(',')
    # sl.reverse()
    s = ""
    for part_name in sl:
        s = s+part_name+" "
    return s.strip()


# subsequent string
def subsequence(s1, s2, m, n):
    b = 0
    a = 0
    while b < m and a < n:
        if s1[b] == s2[a]:
            b += 1
        a += 1
    return b == m


# first pass on the data matching
def fpass(fec_list_names, mit_list_names):
    # fec_list_names is a list of candidate from fec and mit_list_names is a list of candidates from mit
    # output will be a dictionary
    first_dict = collections.defaultdict(list)
    for candidate in mit_list_names:
        flag = 0
        for fec_name in fec_list_names:
            # if i is equal to j we append it to the list of the hashmap for the name in the mit data
            if candidate == fec_name:
                first_dict[candidate].append(fec_name)
                break
            # if they are similar we merge all the possible outcomes and use fuzz ratio technique to find the best match out of it
            elif subsequence(candidate, fec_name, len(candidate), len(fec_name)) or subsequence(fec_name, candidate, len(fec_name), len(candidate)):
                first_dict[candidate].append(fec_name)
            else:
                flag += 1
        # if it found nowhere in the fec data we add ""(blank string) to the dictionary
        if flag >= len(fec_list_names):
            first_dict[candidate].append("")
    return first_dict


# final pass
def out(fpass_dict, fec_list_names):
    # fpass_dict is the dictionary of the output from the fpass
    compare = []
    for key, value in fpass_dict.items():
        # if the length of the dictonary is 1 and contains a null value we use fuzzy wuzzy logic to extract all the possible outcomes.
        # After the possible matches we run the jakkard similarity test to get the closest possible and also confirms it's partial ratio 83% to avoid false postitives
        if len(value) == 1:
            if value[0] == "":
                possible_match = process.extract(key, fec_list_names)
                min_dist = 1.1
                s = ""
                for cvalues in possible_match:
                    dist = nltk.jaccard_distance(set(nltk.ngrams(nltk.word_tokenize(key), n=1)), set(nltk.ngrams(nltk.word_tokenize(cvalues[0]), n=1)))
                    if dist < min_dist and dist <= 0.75 and fuzz.partial_ratio(key, cvalues[0]) >= 83:
                        min_dist = dist
                        s = cvalues[0]
                compare.append([key, s])
            else:
                compare.append([key, value[0]])
        else:
            s = ""
            m = -1
            # if there is already a potential match we just loook for the best from the posible match found using the previous logic
            for i in value:
                if fuzz.ratio(key, i) > m and fuzz.ratio(key, i) >= 86:
                    m = fuzz.ratio(key, i)
                    s = i
            compare.append([key, s])
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
    for i, j in c.items():
        if i == j:
            m += 1
        elif j == "":
            um += 1
        else:
            nm += 1
    return pd.DataFrame([m, um, nm], index=["Matched_fully", "Blank_string", "Matched_partially"])


# Method for normalizing the name
# Moves suffix to end of name
def append_suff(postfix_string):
    for suf in ['Jr.', 'Sr.', 'Iii']:
        sl = re.split(suf, postfix_string)
        if len(sl) > 1:
            seperator = ''
            postfix_string = seperator.join(sl) + " " + suf
    return postfix_string


# normalize the name
def normalize_name(name):
    name = re.sub(r"MR.|DR.|MRS.|MS.|PROF.|PH.D.|PROFESSOR", "", name)
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


# Merges all insignificant candidates for my pleasure... (Done by the percentage of votes recieved and name matching)
# C is the list of candidates
# df is the main dataframe
def merge_insig(d, df):
    keep = []
    other = []
    for candidate in d.keys():
        if(d[candidate] == ''):
            kept = False
            if candidate == 'other':
                kept = True
                other.append(candidate)
            else:
                location = df.loc[(df['candidate'].str.lower() == candidate)]
                byyear = location.groupby('year')
                cvote = byyear.sum()['candidatevotes']
                tvote = byyear.sum()['totalvotes']
                for year in cvote.index:
                    percentage = cvote[year]/tvote[year]*100
                    if percentage > 10:
                        keep.append(candidate)
                        kept = True
                        break
            if not kept:
                other.append(candidate)
                df.loc[(df['candidate'].str.lower() == candidate), 'candidate'] = 'Other'
    return (keep, other)


# Method for generating the dictionary
def nlp_dict(mit_candidate_df, fec_candidate_df):
    final_l = []
    for year in range(1976, 2020, 4):
        fec_candidate_list = [modify(i).lower() for i in fec_candidate_df.loc[(fec_candidate_df["year"] == year), "name"].unique()]
        mit_canidate_list = [i.replace('\\', '').replace('"', '').replace(',', '').lower() for i in mit_candidate_df.loc[(mit_candidate_df["year"] == year), "candidate"].unique()]
        fpass_result = fpass(fec_candidate_list, mit_canidate_list)
        # print("fpass done",year)
        final_l = final_l+out(fpass_result, fec_candidate_list)
        # print("spass done",year)
    return result(final_l)
