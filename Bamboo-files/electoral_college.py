import pandas as pd
import os
import sys
from utils import electoralcollege
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector


class TransformStep(PipelineStep):

    def run_step(self, prev_result, params):
        electoral_college_2016_1996 = prev_result[0]
        electoral_college_2004_1992 = prev_result[1]
        election_2016_1996_democrat = pd.read_excel(electoral_college_2016_1996, sheet_name=16, skiprows=[0, 1, 2, 3, 4])  # gettine the excel sheet containing the election data from 1996 to 2016 for democrat
        election_2004_1992_democrat = pd.read_excel(electoral_college_2004_1992, sheet_name=14, skiprows=[0, 1, 2, 3])  # getting the excel sheet containing the election data for 1992 democrat
        election_2016_1996_republican = pd.read_excel(electoral_college_2016_1996, sheet_name=17, skiprows=[0, 1, 2, 3, 4])  # gettine the excel sheet containing the election data from 1996 to 2016 for republic
        election_2004_1992_republican = pd.read_excel(electoral_college_2004_1992, sheet_name=15, skiprows=[0, 1, 2, 3])  # getting the excel sheet containing the election data for 1992 republic
        election_1992 = electoralcollege(election_2004_1992_democrat.loc[:, ["CLINTON 1992", "Unnamed: 17", "Unnamed: 18"]], election_2004_1992_republican.loc[:, ["BUSH 1992", "Unnamed: 17", "Unnamed: 18"]], 1992)  # creating the electoral collge results for the year 1992
        election_1996 = electoralcollege(election_2016_1996_democrat.loc[:, ["CLINTON 1996", "Unnamed: 27", "Unnamed: 28"]], election_2016_1996_republican.loc[:, ["DOLE 1996", "Unnamed: 27", "Unnamed: 28"]], 1996)  # creating the electoral collge results for the year 1996
        election_2000 = electoralcollege(election_2016_1996_democrat.loc[:, ["GORE 2000", "Unnamed: 22", "Unnamed: 23"]], election_2016_1996_republican.loc[:, ["BUSH 2000", "Unnamed: 22", "Unnamed: 23"]], 2000)  # creating the electoral collge results for the year 2000
        election_2004 = electoralcollege(election_2016_1996_democrat.loc[:, ["KERRY 2004", "Unnamed: 17", "Unnamed: 18"]], election_2016_1996_republican.loc[:, ["BUSH 2004", "Unnamed: 17", "Unnamed: 18"]], 2004)  # creating the electoral collge results for the year 2004
        election_2008 = electoralcollege(election_2016_1996_democrat.loc[:, ["OBAMA 2008", "Unnamed: 12", "Unnamed: 13"]], election_2016_1996_republican.loc[:, ["MCCAIN 2008", "Unnamed: 12", "Unnamed: 13"]], 2008)  # creating the electoral collge results for the year 2008
        election_2012 = electoralcollege(election_2016_1996_democrat.loc[:, ["OBAMA 2012", "Unnamed: 7", "Unnamed: 8"]], election_2016_1996_republican.loc[:, ["ROMNEY 2012", "Unnamed: 7", "Unnamed: 8"]], 2012)  # creating the electoral collge results for the year 2012
        election_2016 = electoralcollege(election_2016_1996_democrat.loc[:, ["CLINTON 2016", "Unnamed: 2", "Unnamed: 3"]], election_2016_1996_republican.loc[:, ["TRUMP 2016", 'Unnamed: 2', "Unnamed: 3"]], 2016)  # creating the electoral collge results for the year 2016
        electoralcollege_1992_2016 = pd.concat([election_1992, election_1996, election_2000, election_2004, election_2008, election_2012, election_2016], axis=0, ignore_index=True)  # merging all the results into one table
        return electoralcollege_1992_2016


class ElectionSenatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Output database connector", name="output-db", dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        sys.path.append(os.getcwd())
        electoral_college_connector = ['electoralcollege-2016', 'electoralcollege-2004']
        dl_step = DownloadStep(connector=electoral_college_connector, connector_path=__file__, force=params.get("force", False))
        xform_step = TransformStep()
        load_step = LoadStep("electoralcollege", connector=params["output-db"], connector_path=__file__, if_exists="append", pk=['year', 'state'], engine="ReplacingMergeTree", engine_params="version")
        return [dl_step, xform_step, load_step]
