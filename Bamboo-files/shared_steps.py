from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from openfec_wrapper import CandidateData


class ExtractFEC_PresidentDataStep(PipelineStep):
    def run_step(self, prev_result, params):
        # Create CandidateData objects president
        p_candidates = CandidateData.presidential_candidates()
        president_fec = p_candidates.dataframe()
        return (prev_result, president_fec)


class ExtractFEC_SenateDataStep(PipelineStep):
    def run_step(self, prev_result, params):
        # Create CandidateData objects president
        s_candidates = CandidateData.senate_candidates()
        senate_fec = s_candidates.dataframe()
        return (prev_result, senate_fec)


class ExtractFEC_HouseDataStep(PipelineStep):
    def run_step(self, prev_result, params):
        # Create CandidateData objects president
        h_candidates = CandidateData.house_candidates()
        house_fec = h_candidates.dataframe()
        return (prev_result, house_fec)
