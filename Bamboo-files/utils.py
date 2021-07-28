import pandas as pd
import numpy as np


def electoral_vote(df, party, dict):
    pd.set_option('mode.chained_assignment', None)
    df.columns = ['geoid', 'party', 'electoralvote']
    df.drop(0, axis=0, inplace=True)  # dropping the null row
    df.reset_index(inplace=True, drop=True)  # reseting the index
    df = df.loc[:50, :]  # considering  the first 51 rows of the state
    df.loc[(df['party'] == "W") | (df['electoralvote'] == 1), 'party'] = party  # Adding the party name where the vote has been secured
    df['geoid'] = df.geoid.apply(lambda x: x.strip('*').strip())  # removing the *'s if present at the end
    different_statename_dc = ['D.C.', 'DC']
    df.loc[(df.geoid.isin(different_statename_dc)), 'geoid'] = "District of Columbia"  # formatting the name in deired format
    df['geoid'] = df.geoid.apply(lambda x: dict[x])  # converting the sate name to geoid
    assert df[(df['party'] == party) & df.geoid.isnull()].empty, "The geoname not found in the dictionary"
    return df


def electoralcollege(democrat_df, republican_df, year):
    geoid_state_dict = {
        "Alabama": "04000US01",
        "Alaska": "04000US02",
        "Arizona": "04000US04",
        "Arkansas": "04000US05",
        "California": "04000US06",
        "Connecticut": "04000US09",
        "District of Columbia": "04000US11",
        "Colorado": "04000US08",
        "Delaware": "04000US10",
        "Florida": "04000US12",
        "Georgia": "04000US13",
        "Hawaii": "04000US15",
        "Idaho": "04000US16",
        "Illinois": "04000US17",
        "Indiana": "04000US18",
        "Iowa": "04000US19",
        "Kansas": "04000US20",
        "Kentucky": "04000US21",
        "Louisiana": "04000US22",
        "Maine": "04000US23",
        "Maryland": "04000US24",
        "Massachusetts": "04000US25",
        "Michigan": "04000US26",
        "Minnesota": "04000US27",
        "Mississippi": "04000US28",
        "Missouri": "04000US29",
        "Montana": "04000US30",
        "Nebraska": "04000US31",
        "Nevada": "04000US32",
        "New Hampshire": "04000US33",
        "New Jersey": "04000US34",
        "New Mexico": "04000US35",
        "New York": "04000US36",
        "North Carolina": "04000US37",
        "North Dakota": "04000US38",
        "Ohio": "04000US39",
        "Oklahoma": "04000US40",
        "Oregon": "04000US41",
        "Pennsylvania": "04000US42",
        "Rhode Island": "04000US44",
        "South Carolina": "04000US45",
        "South Dakota": "04000US46",
        "Tennessee": "04000US47",
        "Texas": "04000US48",
        "Utah": "04000US49",
        "Vermont": "04000US50",
        "Virginia": "04000US51",
        "Washington": "04000US53",
        "West Virginia": "04000US54",
        "Wisconsin": "04000US55",
        "Wyoming": "04000US56"
    }
    democrat_df = electoral_vote(democrat_df, "Democratic", geoid_state_dict)  # getting the data in the derired fromat for democratic dataframe
    republican_df = electoral_vote(republican_df, "Republican", geoid_state_dict)  # getting the data in the desired format for republican dataframe
    democrat_df.dropna(axis=0, inplace=True)
    republican_df.dropna(axis=0, inplace=True)
    df = pd.concat([democrat_df, republican_df], axis=0, ignore_index=True)  # merging both dataframe
    df['year'] = year  # creting a new column of the year
    df['electoralvote'] = df['electoralvote'].astype(np.int64)  # specifying the data type as integer
    cols = ['year', 'geoid', 'party', 'electoralvote']  # arranging the order of columns
    df = df[cols]
    return df


NAMES_MAP = {
    'Kit Bond': 'S6MO00289',
    'Jerry Brekke': 'S6MNXXX76',
    'John Grady': 'S6FLXXX76',
    'Dean Barkley': 'S4MNXXX96',
    'David Durenberger': 'S8MN00131',
    'Alex Smith': 'S6ILXXX78',
    'Dwight Jensen': 'S6IDXXX78',
    'Jim Sykes': 'S2AKXXX02',
    'Tom Kelly': 'S8ARXXX78',
    'John Graham Black': 'S8ARXX078',
    'Donald Stewart': 'S8AL00043',
    'Gale Mcgee': 'S6WYXXX76',
    'Stanley York': 'S2WIXXX76',
    'Blank Vote': 'S99999998',
    'Alan Steelman': 'S6TXXXX76',
    'Robert Stroup': 'S6NDXXX76',
    'David Towell': 'S6NVXXX76',
    'William Quinn': 'S6HIXXX76',
    'Gloria Schaffer': 'S6CTXXX76'
}