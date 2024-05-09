import os
import shutil


class DirectoryUtil:

    @staticmethod
    def set_directory(config, deploy_dependencies=True):
        source_path = config["source_path"]
        deploy_path = config["deploy_path"]
        source_app_path = config["source_app_path"]
        deploy_app_path = config["deploy_app_path"]

        if os.path.exists(deploy_path):
            shutil.rmtree(deploy_path)
        os.mkdir(deploy_path)

        if deploy_dependencies:
            internal_dependencies = config['internal_dependencies']
            external_dependencies = config['external_dependencies']
            file_dependencies = config['file_dependencies']

            for dependency in external_dependencies:
                source_dependency_path = os.path.join(source_path, dependency)
                deploy_dependency_path = os.path.join(deploy_path, dependency)
                if os.path.isdir(source_dependency_path):
                    DirectoryUtil.copy(source_dependency_path, deploy_dependency_path)
                else:
                    shutil.copyfile(source_dependency_path, deploy_dependency_path)


            for dependency in internal_dependencies:
                source_dependency_path = os.path.join(source_path, source_app_path, dependency)
                deploy_dependency_path = os.path.join(deploy_path, deploy_app_path, dependency)
                DirectoryUtil.copy(source_dependency_path, deploy_dependency_path)

            for dependency in file_dependencies:
                source_dependency_path = os.path.join(source_path, source_app_path, dependency)
                deploy_dependency_path = os.path.join(deploy_path, deploy_app_path, dependency)
                shutil.copyfile(source_dependency_path, deploy_dependency_path)

    @staticmethod
    def copy(src, destination):
        shutil.copytree(src, destination)
