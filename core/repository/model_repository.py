from core.entities.model import Model


class ModelRepository:

    def add(self, model: Model):
        model.save()

    def get(self, name):
        model = Model.get(name)
        return model
