import settings


class SettingsLocal(settings):
    LIVY_URL = 'http://' + 'localhost' + ':8998'
    AWS_CREDENTIALS = {
        "access_key_id": "",
        "secret_access_key": "",
        "region": "",
    }
