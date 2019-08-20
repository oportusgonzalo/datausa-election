import pandas as pd
import numpy as np


def election_democrat(df):
    df.columns = ['state', "status", "electoralvote_democrat"]
    df.drop(0, axis=0, inplace=True)  # dropping the null row
    df.reset_index(inplace=True, drop=True)  # reseting the index
    df = df.loc[:50, :]  # considering  the first 51 rows of the state
    df.loc[(df['status'] != "W"), 'electoralvote_democrat'] = 0  # repalcing the electoral vote were they haven't got any seat
    df.drop('status', axis=1, inplace=True)  # dropping the status column
    df.set_index('state', inplace=True, drop=False)  # setting the sate as the index
    df['electoralvote_democrat'] = df['electoralvote_democrat'].astype(np.int64)  # specifying the data type as integer
    return df


def election_republican(df):
    df.columns = ['state', "status", "electoralvote_republican"]
    df.drop(0, axis=0, inplace=True)  # dropping the null row
    df.reset_index(inplace=True, drop=True)  # reseting the index
    df = df.loc[:50, :]  # considering  the first 51 rows of the state
    df.set_index('state', inplace=True)  # setting the sate as the index
    df.loc[(df['status'] != "W") & (df['electoralvote_republican'] != 1), 'electoralvote_republican'] = 0   # repalcing the electoral vote were they haven't got any seat with an edge  case in 2016 where they only got the one seat in maine
    df.drop('status', axis=1, inplace=True)  # dropping the status column
    df['electoralvote_republican'] = df['electoralvote_republican'].astype(np.int64)  # specifying the data type as integer
    return df


def electoralcollege(democrat_df, republican_df, year):
    democrat_df = election_democrat(democrat_df)  # getting the data in the derired fromat for democratic dataframe
    republican_df = election_republican(republican_df)  # getting the data in the desired format for republican dataframe
    df = pd.concat([democrat_df, republican_df], axis=1)  # merging both dataframe
    df['year'] = year  # creting a new column of the year
    df.reset_index(inplace=True, drop=True)  # resetting the index
    return df
