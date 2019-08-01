import collections
import re
import nltk
import datetime
from fuzzywuzzy import process, fuzz


# Modification method
# bool value is true when we want reverse the string that is wehn the candidate name is not from presidential data
def modify_fecname(name, bool):
    sl = name.replace('"', '').split(',')
    if bool:
        sl.reverse()
    modified_name = ""
    for part_name in sl:
        modified_name = modified_name + part_name + " "
    return modified_name.strip()


# subsequent string
# finding a matching string even if there are some intermediate unmatching word
# for example:- Hal furman will match with Harold W ii Furman regardless of additional character present in it
def interruptable_substring_subsequence(s1, s2, s1_length, s2_length):
    b = 0
    a = 0
    while b < s1_length and a < s2_length:
        if s1[b] == s2[a]:
            b += 1  # increments the b when there is a match of s1 and s2
        a += 1  # iterates through the s2
    return b == s1_length  # if s1 is present in s2 the length of traversed should be same as length of string


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
            elif interruptable_substring_subsequence(candidate, fec_name, len(candidate), len(fec_name)) or interruptable_substring_subsequence(fec_name, candidate, len(fec_name), len(candidate)):
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
    for mit_name, fec_possible_match in fpass_dict.items():
        # if the length of the dictonary is 1 and contains a null value we use fuzzy wuzzy logic to extract all the possible outcomes.
        # After the possible matches we run the jakkard similarity test to get the closest possible and also confirms it's partial ratio 83% to avoid false postitives
        if len(fec_possible_match) == 1:
            if fec_possible_match[0] == "":
                possible_match = process.extract(mit_name, fec_list_names)
                min_dist = 1.1
                match_string = ""
                for cvalues in possible_match:
                    dist = nltk.jaccard_distance(set(nltk.ngrams(nltk.word_tokenize(mit_name), n=1)), set(nltk.ngrams(nltk.word_tokenize(cvalues[0]), n=1)))
                    if dist < min_dist and dist <= 0.75 and fuzz.partial_ratio(mit_name, cvalues[0]) >= 83:
                        min_dist = dist
                        match_string = cvalues[0]
                compare.append([mit_name, match_string])
            else:
                compare.append([mit_name, fec_possible_match[0]])
        else:
            match_string = ""
            match_ratio = -1
            # if there is already a potential match we just loook for the best from the posible match found using the previous logic
            for possible_outcome in fec_possible_match:
                if fuzz.ratio(mit_name, possible_outcome) > match_ratio and fuzz.ratio(mit_name, possible_outcome) >= 86:
                    match_ratio = fuzz.ratio(mit_name, possible_outcome)
                    match_string = possible_outcome
            compare.append([mit_name, match_string])
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
# check_dict is the dictionary of Mit data and it's match on the fec data
def check(check_dict):
    matched = 0
    unmatched = 0
    partialmatched = 0
    for mit_name, fec_name in check_dict.items():
        if mit_name == fec_name:
            matched += 1
        elif fec_name == "":
            unmatched += 1
        else:
            partialmatched += 1
    return [matched, unmatched, partialmatched]


# Method for normalizing the name
# formating name MIT data
def formatname_mitname(name):
    name = name.replace('\\', '').strip('\"')
    temp = re.sub('\".+\"', '', name)
    return ' '.join(temp.split())


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
    name = re.sub(r"MR.|DR.|MRS.|MS.|PROF.|PH.D.|PROFESSOR|SENATOR", "", name)
    a = name.find('(')  # finding the location of '(' and ')' in a string for exaple Gearald ford (NOT A President candidate)
    b = name.find(')')  # using these we remove the "(.........)" any kind of this string
    c = name.find('/')  # removing the Vice President's name if give in the FEC data such as candidate / vice president of candidate
    if a != -1 and b != -1:
        name = name[:a] + name[b + 1:]
    if c != -1:
        name = name.split('/')[0]
    name_list = name.split(',')
    if len(name_list) == 1:
        return name.title()
    ln = name_list[0]
    fn = ' '.join(name_list[1].split())
    return ' '.join(append_suff((fn + " " + ln).title()).split())


# Merges all insignificant candidates (Done by the percentage of votes recieved and name matching)
# d is the dictionary of candidates
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
                    percentage = cvote[year] / tvote[year] * 100
                    if percentage > 5:
                        keep.append(candidate)
                        kept = True
                        break
            if not kept:
                other.append(candidate)
                df.loc[(df['candidate'].str.lower() == candidate), 'candidate'] = 'Other'
    return (keep, other)


# Method for generating the dictionary
# gap variables gives the difference between the election years
# bool is true if it's presidential data by state else it is false
def nlp_dict(mit_candidate_df, fec_candidate_df, gap, bool):
    final_l = []
    d = datetime.date.today()
    for year in range(1976, (d.year + 1), gap):
        if bool:
            fec_candidate_list = [modify_fecname(candidate, False).lower() for candidate in fec_candidate_df.loc[(fec_candidate_df["year"] == year), "name"].unique()]
            mit_canidate_list = [candidate.replace('\\', '').replace('"', '').replace(',', '').lower() for candidate in mit_candidate_df.loc[(mit_candidate_df["year"] == year), "candidate"].unique()]
        else:
            fec_candidate_list = [modify_fecname(candidate, True).lower() for candidate in fec_candidate_df.loc[(fec_candidate_df["year"] == year), "name"].unique()]
            mit_canidate_list = [formatname_mitname(candidate).replace('.', '').lower() for candidate in mit_candidate_df.loc[(mit_candidate_df["year"] == year), "candidate"].unique()]
        fpass_result = fpass(fec_candidate_list, mit_canidate_list)
        # print("fpass done",year)
        final_l = final_l + out(fpass_result, fec_candidate_list)
        # print("spass done",year)
    return result(final_l)
