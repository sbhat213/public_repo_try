from types import SimpleNamespace


class StepConfig:

    def __init__(self, data):
        obj = SimpleNamespace(**data)
        self.name = obj.name
        self.steps = obj.steps
        self.depends_on = obj.depends_on
        self.global_map = GlobalMap(obj.global_map)


class GlobalMap:

    def __init__(self, map):
        if "core" in map:
            self.core = map["core"]
        else:
            self.core = {}
        if "audit" in map:
            self.audit = map["audit"]
        else:
            self.audit = {}
