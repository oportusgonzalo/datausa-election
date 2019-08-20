import pandas as pd
import os
import sys
from utils import electoralcollege
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector


class TransformStep(PipelineStep):

    def run_step(self, prev_result, params):
        # make sure to perform the formating from the excel sheet accrodingly to get the accurate results
        if params["connector"] == "electoralcollege-2016":
            election_democrat = pd.read_excel(prev_result, sheet_name=16, skiprows=[0, 1, 2, 3, 4])  # gettine the excel sheet containing the election data from 1996 to 2016 for democrat
            election_republican = pd.read_excel(prev_result, sheet_name=17, skiprows=[0, 1, 2, 3, 4])  # gettine the excel sheet containing the election data from 1996 to 2016 for republic
        else:
            election_democrat = pd.read_excel(prev_result, sheet_name=14, skiprows=[0, 1, 2, 3])  # getting the excel sheet containing the election data for 1992 democrat
            election_republican = pd.read_excel(prev_result, sheet_name=15, skiprows=[0, 1, 2, 3])  # getting the excel sheet containing the election data for 1992 republic
        # gathering the parameter values according to the requirements
        year = params["year"]
        campaign_d = params["campaign-d"]
        campaign_r = params["campaign-r"]
        col1 = "Unnamed: " + params["col1_number"]
        col2 = "Unnamed: " + params["col2_number"]
        electoral_college = electoralcollege(election_democrat.loc[:, [campaign_d, col1, col2]], election_republican.loc[:, [campaign_r, col1, col2]], year)  # getting the result
        return electoral_college


class ElectionSenatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name="connector", dtype=str),
            Parameter(name="year", dtype=int),
            Parameter(name="campaign-d", dtype=str),
            Parameter(name="campaign-r", dtype=str),
            Parameter(name="col1_number", dtype=str),
            Parameter(name="col2_number", dtype=str),
            Parameter(label="Output database connector", name="output-db", dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        sys.path.append(os.getcwd())
        dl_step = DownloadStep(connector=params["connector"], connector_path=__file__, force=params.get("force", False))
        xform_step = TransformStep()
        load_step = LoadStep("electoralcollege", connector=params["output-db"], connector_path=__file__, if_exists="append", pk=['year', 'geoid'], engine="ReplacingMergeTree", engine_params="version")
        return [dl_step, xform_step, load_step]
