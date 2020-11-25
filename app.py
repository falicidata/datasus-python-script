
from pysus_download import PysusDownload
from repo_service import RepoService


modules = ['CIHA']
estados = ['SP']
anos = [2018]
months = [1]


def incluir():
    for module in modules:
        pd = PysusDownload(module, anos, estados, months)
        res = pd.download()
        return res


incluir()
