import importlib
from bson.objectid import ObjectId
from repo_service import RepoService
import pandas as pd
import json


class PysusDownload:

    def __init__(self, module, anos, estados, meses):
        self.module = module
        self.estados = estados
        self.meses = meses
        self.anos = anos

    def download_json(self):
        mod = importlib.import_module("pysus.online_data." + self.module)
        banco = {}

        if self.meses is None:
            for y in self.anos:
                for uf in self.estados:
                    try:
                        banco[uf, y] = mod.download(state=uf, year=y)
                        print("Banco " + str(self.module) + " ano " +
                              str(y) + " estado " + str(uf) + " baixado!")
                    except Exception as e:
                        print("Banco " + str(self.module) +
                              " ano " + str(y) + " estado " + str(uf))
                        print("Error: ")
                        print(e)

        else:
            for y in self.anos:
                for uf in self.estados:
                    for m in self.meses:
                        try:
                            banco[uf, y, m] = mod.download(
                                state=uf, year=y, month=m)
                            print("Banco " + str(self.module) + " ano " + str(y) +
                                  " mes " + str(m) + " estado " + str(uf) + " baixado!")
                        except Exception as e:
                            print("Banco " + str(self.module) + " ano " +
                                  str(y) + " mes " + str(m) + " estado " + str(uf))
                            print("Error:")
                            print(e)

        todos = pd.concat({k: pd.DataFrame.from_dict(v)
                           for k, v in banco.items()}, axis=0).reset_index()
        df = pd.DataFrame(todos)

        result = df.to_json(orient="records")
        parsed = json.loads(result)
        ser = json.dumps(parsed, indent=4)
        return ser

    def download_csv(self):
        mod = importlib.import_module("pysus.online_data." + self.module)
        banco = {}

        if self.meses is None:
            for y in self.anos:
                for uf in self.estados:
                    try:
                        banco[uf, y] = mod.download(state=uf, year=y)
                        print("Banco " + str(self.module) + " ano " +
                              str(y) + " estado " + str(uf) + " baixado!")
                    except Exception as e:
                        print("Banco " + str(self.module) +
                              " ano " + str(y) + " estado " + str(uf))
                        print("Error: ")
                        print(e)

        else:
            for y in self.anos:
                for uf in self.estados:
                    for m in self.meses:
                        try:
                            banco[uf, y, m] = mod.download(
                                state=uf, year=y, month=m)
                            print("Banco " + str(self.module) + " ano " + str(y) +
                                  " mes " + str(m) + " estado " + str(uf) + " baixado!")
                        except Exception as e:
                            print("Banco " + str(self.module) + " ano " +
                                  str(y) + " mes " + str(m) + " estado " + str(uf))
                            print("Error:")
                            print(e)

        todos = pd.concat({k: pd.DataFrame.from_dict(v)
                           for k, v in banco.items()}, axis=0).reset_index()
        df = pd.DataFrame(todos)

        df.to_csv(self.module+'.csv')

    def download_old(self):

        mod = importlib.import_module("pysus.online_data." + self.module)
        banco = {}

        if self.meses is None:
            for y in self.anos:
                for uf in self.estados:
                    try:
                        banco[uf, y] = mod.download(state=uf, year=y)
                        print("Banco " + str(self.module) + " ano " +
                              str(y) + " estado " + str(uf) + " baixado!")
                    except Exception as e:
                        print("Banco " + str(self.module) +
                              " ano " + str(y) + " estado " + str(uf))
                        print("Error: ")
                        print(e)

        else:
            for y in self.anos:
                for uf in self.estados:
                    for m in self.meses:
                        try:
                            banco[uf, y, m] = mod.download(
                                state=uf, year=y, month=m)
                            print("Banco " + str(self.module) + " ano " + str(y) +
                                  " mes " + str(m) + " estado " + str(uf) + " baixado!")
                        except Exception as e:
                            print("Banco " + str(self.module) + " ano " +
                                  str(y) + " mes " + str(m) + " estado " + str(uf))
                            print("Error:")
                            print(e)

        objs = self.cria_objs(banco.items())
        return objs

    def download(self):
        mod = importlib.import_module("pysus.online_data." + self.module)
        objs = []
        count = 0
        repo = RepoService(self.module)
        index_response = repo.create_index_sus()
        print("index response: ", index_response)
        
        if self.meses is None:
            for y in self.anos:
                for uf in self.estados:
                    try:
                        banco = {}
                        banco[uf, y] = mod.download(state=uf, year=y)
                        print("Banco " + str(self.module) + " ano " +
                              str(y) + " estado " + str(uf) + " baixado!")
                        self.cria_objs_params(banco.items(), uf, y)
                    except Exception as e:
                        print("Banco " + str(self.module) +
                              " ano " + str(y) + " estado " + str(uf))
                        print("Error: ")
                        print(e)

        else:
            for y in self.anos:
                for uf in self.estados:
                    for m in self.meses:
                        try:
                            banco = {}
                            banco[uf, y, m] = mod.download(
                                state=uf, year=y, month=m)
                            banco_len = len(banco.items())
                            print("Banco " + str(self.module) + " ano " + str(y) +
                                  " mes " + str(m) + " estado " + str(uf) + " baixado!")
                            objs_params = self.cria_objs_params(
                                banco.items(), uf, y, m)
                            for obj_param in objs_params:
                                objs.append(obj_param)
                                count = count + 1

                        except Exception as e:
                            print("Banco " + str(self.module) + " ano " +
                                  str(y) + " mes " + str(m) + " estado " + str(uf))
                            print("Error:")
                            print(e)

        
        
        # objs = self.cria_objs(banco.items())
        # return objs

    def cria_objs_params(self, items, uf, ano, mes):
        objs = []
        limit = 50000
        repo = RepoService(self.module)
        for k, v in items:
            qtd_arquivos = len(v)
            if qtd_arquivos > 1 and not hasattr(v, 'columns'):
                for index_arquivo in range(0, qtd_arquivos):
                    columns = v[index_arquivo].columns
                    qtd_linhas = len(v[index_arquivo])
                    print(qtd_linhas)

                    for linha in range(0, qtd_linhas):
                        obj = {}
                        obj['id'] = linha
                        obj['uf'] = str(uf)
                        obj['ano'] = str(ano)
                        obj['mes'] = str(mes)

                        for column in columns:
                            value = str(v[index_arquivo][column][linha])
                            if value:
                                obj[column] = str(
                                    v[index_arquivo][column][linha])

                        print('linha: ' + str(linha))
                        objs.append(obj)

                        if len(objs) == limit:
                            repo.insert(objs)
                            objs = []

                        print('linha ' + str(linha) + ' add')

                repo.insert(objs)
                return objs

            else:
                columns = v.columns
                qtd_linhas = len(v)
                print(qtd_linhas)
                for linha in range(1, qtd_linhas):
                    obj = {}
                    obj['id'] = linha
                    obj['uf'] = str(uf)
                    obj['ano'] = str(ano)
                    obj['mes'] = str(mes)
                    for column in columns:
                        value = str(v[column][linha])
                        if value:
                            obj[column] = str(v[column][linha])

                    print('linha: ' + str(linha))
                    objs.append(obj)
                    if len(objs) == limit:
                        repo.insert(objs)
                        objs = []
                    print('linha ' + str(linha) + ' add')
            repo.insert(objs)
            return objs

    def cria_objs(self, items):
        objs = []
        repo = RepoService(self.module)
        for k, v in items:
            qtd_arquivos = len(v)
            if qtd_arquivos > 1 and not hasattr(v, 'columns'):
                for index_arquivo in range(0, qtd_arquivos):
                    columns = v[index_arquivo].columns
                    qtd_linhas = len(v[index_arquivo])
                    print(qtd_linhas)

                    for linha in range(0, qtd_linhas):
                        obj = {}
                        obj['id'] = linha

                        for column in columns:
                            value = str(v[index_arquivo][column][linha])
                            if value:
                                obj[column] = str(
                                    v[index_arquivo][column][linha])

                        print('linha: ' + str(linha))
                        objs.append(obj)
                        print('linha ' + str(linha) + ' add')
                return objs

            else:
                columns = v.columns
                qtd_linhas = len(v)
                print(qtd_linhas)
                for linha in range(1, qtd_linhas):
                    obj = {}
                    obj['id'] = linha
                    for column in columns:
                        value = str(v[column][linha])
                        if value:
                            obj[column] = str(v[column][linha])

                    print('linha: ' + str(linha))
                    objs.append(obj)
                    print('linha ' + str(linha) + ' add')
            return objs

    def cria_objs_arquivos(self, items):
        objs = []
        for k, v in items:
            qtd_arquivos = len(v)
            for index_arquivo in range(0, qtd_arquivos):
                columns = v[index_arquivo].columns
                qtd_linhas = len(v[index_arquivo])
                print(qtd_linhas)
                for linha in range(0, qtd_linhas):
                    obj = {}
                    obj['id'] = str(linha)
                    for column in columns:
                        obj[column] = str(v[column][linha])
                    objs.append(obj)
            return objs
