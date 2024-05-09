from aws_cdk import core, aws_lambda as _lambda, aws_s3 as s3, aws_iam as iam

from provision.provision_action.iam_role import IAmRole
from config.config_parser import ConfigParser


class JobIAmRole(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

    def createAll(self):
        self.iam_roles = {}
        roles_config = JobIAmRole.iam_role_config()
        for role_name in roles_config:
            self.iam_roles[role_name] = IAmRole.create_role(self, role_name)

    @staticmethod
    def iam_role_config():
        path = f"E:\\Shared\\Indegene\\aws_provision\\config\\"
        roles_config = ConfigParser.parse_config(path + "role_policy.json")
        return roles_config
