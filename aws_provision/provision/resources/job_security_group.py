from aws_cdk import core
from provision.provision_action.security_groups import SecurityGroups
from config.config_parser import ConfigParser


class JobSecurityGroup(core.Stack):
    def __init__(self, scope: core.Construct, id: str, vpc=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)


    def createAll(self):
        self.security_group_map = {}
        security_groups_config = JobSecurityGroup.security_group_config()
        for security_group_name in security_groups_config:
            self.security_group_map[security_group_name] = SecurityGroups.create_security_group(self,
                                                                                                security_group_name)

        for security_group_name in security_groups_config:
            SecurityGroups.add_inbound_rules(self,security_group_name,self.security_group_map)
            SecurityGroups.add_outbound_rules(self,security_group_name,self.security_group_map)

    @staticmethod
    def security_group_config():
        path = f"E:\\Shared\\Indegene\\aws_provision\\config\\"
        security_groups_config = ConfigParser.parse_config(path + "security_group_config.json")
        return security_groups_config
