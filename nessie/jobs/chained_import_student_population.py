from flask import current_app as app
from nessie.jobs.background_job import ChainedBackgroundJob
from nessie.jobs.create_calnet_schema import CreateCalNetSchema
from nessie.jobs.create_coe_schema import CreateCoeSchema
from nessie.jobs.create_l_s_schema import CreateLSSchema
from nessie.jobs.generate_asc_profiles import GenerateAscProfiles
from nessie.jobs.import_asc_athletes import ImportAscAthletes
from nessie.jobs.import_calnet_data import ImportCalNetData


class ChainedImportStudentPopulation(ChainedBackgroundJob):
    def __init__(self):
        steps = [
            CreateCoeSchema(),
            ImportAscAthletes(),
            GenerateAscProfiles(),
        ]
        if app.config['L_AND_S_ENABLED']:
            steps.append(CreateLSSchema())
        steps += [
            ImportCalNetData(),
            CreateCalNetSchema(),
        ]
        super().__init__(steps=steps)
