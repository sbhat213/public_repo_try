from core.entities.file_model import FileModel


class FileModelRepository:

    def add(self, file_model: FileModel):
        file_model.save()

    def get(self, name):
        file_model = FileModel.get(name)
        return file_model
