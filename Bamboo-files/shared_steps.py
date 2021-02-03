from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from openfec_wrapper import CandidateData


class ExtractFECStep(PipelineStep):
    PRESIDENT = 'P'
    SENATE = 'S'
    HOUSE = 'H'

    def __init__(self, candidate_type, **kwargs):
        super().__init__(**kwargs)
        self.candidate_type = candidate_type
       # TODO can check values here and raise ValueError if candidate_type is invalid...

    def run_step(self, prev_result, params):
        p_candidates = CandidateData(self.candidate_type)
        fec_df = p_candidates.dataframe()
        return (prev_result, fec_df)

class DirectStep(PipelineStep):
    def run_step(self, prev, params):
        return '/datausa-election/Bamboo-files/1976-2020-senate.tab'
        #return '/datausa-election/Bamboo-files/1976-2020-president.tab'
