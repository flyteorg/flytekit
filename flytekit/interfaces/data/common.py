class DataProxy(object):
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def exists(self, path):
        """
        :param path:
        :rtype: bool: whether the file exists or not
        """
        pass

    def download_directory(self, remote_path, local_path):
        """
        :param Text remote_path:
        :param Text local_path:
        """
        pass

    def download(self, remote_path, local_path):
        """
        :param Text remote_path:
        :param Text local_path:
        """
        pass

    def upload(self, file_path, to_path):
        """
        :param Text file_path:
        :param Text to_path:
        """
        pass

    def upload_directory(self, local_path, remote_path):
        """
        :param Text local_path:
        :param Text remote_path:
        """
        pass

    def get_random_path(self):
        """
        :rtype: Text
        """
        pass

    def get_random_directory(self):
        """
        :rtype: Text
        """
        pass
