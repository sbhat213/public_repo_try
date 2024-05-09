from aws_cdk import core, aws_ec2 as ec2
from aws_cdk.aws_ec2 import Vpc, CfnRouteTable, RouterType, CfnRoute, CfnInternetGateway, CfnVPCGatewayAttachment, \
    CfnSubnet, CfnEIP, CfnNatGateway, CfnSubnetRouteTableAssociation, CfnSecurityGroup, CfnInstance, \
    CfnSecurityGroupIngress

from config.config_parser import ConfigParser


class VpcStack(core.Stack):

    def create_vpc(self, vpc_name):
        self.vpc = Vpc(
            self, vpc_name, cidr='10.0.0.0/16', max_azs=2, nat_gateways=1,
            subnet_configuration=[ec2.SubnetConfiguration(
                subnet_type=ec2.SubnetType.PUBLIC,
                name="IDP-Public-Bastion-1a",
                cidr_mask=20
            ),
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PRIVATE,
                    name="IDP-Pvt-",
                    cidr_mask=24
                )
            ], enable_dns_support=True,
            enable_dns_hostnames=True
        )

        VpcStack.create_vpc_endpoints(self, vpc_name)

    def create_managed_prefix_list(self, vpc_name):
        self.prefix_list_map = {}
        vpc_config = VpcStack.get_vpc_config(vpc_name)
        for prefix_list_config in vpc_config['managed_prefix_list']:
            self.prefix_list_map[prefix_list_config['prefix_list_name']] = ec2.CfnPrefixList(
                self, prefix_list_config['prefix_list_name'],
                prefix_list_name=prefix_list_config['prefix_list_name'],
                address_family=prefix_list_config['address_family'],
                max_entries=prefix_list_config['max_entries'],
            )

    def create_vpc_endpoints(self, vpc_name):
        vpc_config = VpcStack.get_vpc_config(vpc_name)
        self.vpc_endpoints = {}
        for vpc_endpoint_config in vpc_config['endpoints']:
            subnet_ids = []
            route_ids = []
            if vpc_endpoint_config['endpoint_type'] == "Interface":
                subnet_ids = [subnet.subnet_id for subnet in self.vpc.private_subnets]
            if vpc_endpoint_config['endpoint_type'] == "Gateway":
                route_ids = [subnet.route_table.route_table_id for subnet in self.vpc.private_subnets]
            print(route_ids)
            self.vpc_endpoints[vpc_endpoint_config['service_name']] = ec2.CfnVPCEndpoint(
                self, vpc_endpoint_config['endpoint_type'] + vpc_endpoint_config['service_name'],
                vpc_id=self.vpc.vpc_id,
                vpc_endpoint_type=vpc_endpoint_config['endpoint_type'],
                service_name=vpc_endpoint_config['service_name'],
                subnet_ids=subnet_ids,
                route_table_ids=route_ids
            )

    @staticmethod
    def get_vpc_config(vpc_name):
        path = f"E:\\Shared\\Indegene\\aws_provision\\config\\"
        roles_config = ConfigParser.parse_config(path + "vpc_config.json")
        return roles_config[vpc_name]
