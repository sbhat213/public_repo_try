from aws_cdk import aws_ec2 as ec2, core
from config.config_parser import ConfigParser
from aws_cdk.aws_ec2 import Vpc, CfnRouteTable, RouterType, CfnRoute, CfnInternetGateway, CfnVPCGatewayAttachment, \
    CfnSubnet, CfnSubnetRouteTableAssociation, CfnSecurityGroup, CfnInstance, CfnSecurityGroupIngress,CfnSecurityGroupEgress


class SecurityGroups:

    def create_security_group(self, security_group_name):
        security_group_config = SecurityGroups.get_security_group_config(security_group_name)
        return CfnSecurityGroup(
            self, security_group_name, vpc_id=self.vpc.vpc_id,
            group_description=security_group_config['group_description'], group_name=security_group_name
        )

    def add_inbound_rules(self, security_group_name, source_security_group_map):
        security_group_config = SecurityGroups.get_security_group_config(security_group_name)
        for inbound_rule in security_group_config['Inbound']:
            CfnSecurityGroupIngress(self, inbound_rule['id'], ip_protocol=inbound_rule['ip_protocol'],
                                    source_security_group_id=source_security_group_map[
                                        inbound_rule['source_security_group_name']].attr_group_id,
                                    from_port=inbound_rule['from_port'], to_port=inbound_rule['to_port'],
                                    group_id=source_security_group_map[security_group_name].attr_group_id)

    def add_outbound_rules(self, security_group_name, source_security_group_map):
        security_group_config = SecurityGroups.get_security_group_config(security_group_name)
        for outbound_rule in security_group_config['Outbound']:
            outbound_rule_generic_config = {}
            if 'source_security_group_name' in outbound_rule:
                outbound_rule_generic_config['destination_security_group_id']=source_security_group_map[outbound_rule['source_security_group_name']].attr_group_id
            # if 'destination_prefix_list_name' in outbound_rule:
            #     outbound_rule_generic_config['destination_prefix_list_id'] = self.prefix_list_map[outbound_rule['destination_prefix_list_name']].attr_prefix_list_id
            # print(outbound_rule_generic_config)
            CfnSecurityGroupEgress(self,outbound_rule['id'],ip_protocol=outbound_rule['ip_protocol'],
                                   from_port=outbound_rule['from_port'],to_port=outbound_rule['to_port'],
                                   group_id=source_security_group_map[security_group_name].attr_group_id,
                                    **outbound_rule_generic_config
                                   )


    @staticmethod
    def get_security_group_config(security_group_name):
        path = f"E:\\Shared\\Indegene\\aws_provision\\config\\"
        roles_config = ConfigParser.parse_config(path + "security_group_config.json")
        return roles_config[security_group_name]
