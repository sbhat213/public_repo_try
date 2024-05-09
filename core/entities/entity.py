from pynamodb.models import Model
from settings import Settings

class Entity(Model):

    def __init__(self, hash_key=None, range_key=None, **model_attributes):
        pass


class EntityMeta:
    aws_access_key_id = Settings.AWS_CREDENTIALS["access_key_id"]
    aws_secret_access_key = Settings.AWS_CREDENTIALS["secret_access_key"]
    region = Settings.AWS_CREDENTIALS["region"]

    # ToDo : Enable host url when testing on local
    # host = 'http://localhost:8001'
