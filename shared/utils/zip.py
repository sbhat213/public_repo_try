import os
import zipfile
from zipfile import ZipFile


class ZIP:

    @staticmethod
    def get_all_file_paths(directory):
        # initializing empty file paths list
        file_paths = []

        # crawling through directory and subdirectories
        for root, directories, files in os.walk(directory):
            for filename in files:
                # join the two strings in order to form the full file_path.
                file_path = os.path.join(root, filename)
                file_paths.append(file_path)

                # returning all file paths
        return file_paths

    @staticmethod
    def make_zip(directory, zip_name):
        # calling function to get all file paths in the directory
        file_paths = ZIP.get_all_file_paths(directory)
        with ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zip:
            # writing each file one by one
            for file in file_paths:
                file_name = file.replace(directory, '')
                zip.write(file, file_name)
