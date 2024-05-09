class Emr:

    def __init__(self, cluster_id, cluster_arn, cluster_name, livy_url,status,creation_time):
        self.cluster_id = cluster_id
        self.cluster_arn = cluster_arn
        self.cluster_name = cluster_name
        self.livy_url = livy_url
        self.status = status
        self.creation_time = creation_time

    # def __init__(self):
    #     self.cluster_id = ""
    #     self.cluster_arn = ""
    #     self.cluster_name = ""
    #     self.livy_url = ""
    #     self.status = ""
    #     self.creation_time = ""


