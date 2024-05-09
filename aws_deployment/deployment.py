import os
from aws_deployment import injector


class Deployment:

    @staticmethod
    def deploy(pipeline):
        result = {}
        print(pipeline)
        global_settings = pipeline['global']
        for step_config in pipeline['steps']:
            deploy = step_config["deploy"]
            if deploy == "True":
                step_config = Deployment.merge_settings(step_config, global_settings)
                result = injector.get(step_config['action']).deploy(result, step_config)
        return result

    @staticmethod
    def merge_settings(step_config, global_settings):
        for k, v in global_settings.items():
            if k in step_config:
                if isinstance(v, type([])):
                    step_config[k].extend(v)
            else:
                if k == "deploy_path":
                    step_config[k] = os.path.join(v, step_config["name"])
                else:
                    step_config[k] = v
        return step_config
