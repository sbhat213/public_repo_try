import datetime
import json

import requests


class Requests:
    def post(self, url,  data, headers={}):
        resp = requests.post(url, json=data, headers=headers)
        return resp

    def get(self, url, params={}, headers={}):
        resp = requests.get(url, params, headers=headers, timeout=100)
        return resp

    def delete(self, url, params={}, headers={}):
        resp = requests.delete(url, headers=headers,timeout=100)
        return resp


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if obj is None:
            return None
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(type(obj), type(datetime.datetime)):
            return str(obj)

        return json.JSONEncoder.default(self, obj)
