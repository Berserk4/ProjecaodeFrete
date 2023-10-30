import os

class DataSet:
    def __init__(self, caminho, delimiter, usecols=None, dtype=None):
        self.caminho = caminho
        self.arquivos = os.listdir(caminho)
        self.read_args = {
            'encoding': 'latin1',
            'lineterminator': '\n',
            'delimiter': delimiter,
            'index_col': False,
            'usecols': usecols,
            'dtype': dtype
        }

class V2DataSet(DataSet):
    def __init__(self, caminho):
        super().__init__(caminho, delimiter=';')

class DescProdDataSet(DataSet):
    def __init__(self, caminho):
        cols_idx = [0, 4, 13, 14]
        dtype = {13: str}
        super().__init__(caminho,delimiter=';',usecols=cols_idx, dtype=dtype)

class BQDataSet(DataSet):
    def __init__(self, caminho):
        super().__init__(caminho,delimiter=',')

class ProjDataSet(DataSet):
    def __init__(self, caminho):
        super().__init__(caminho,delimiter=';')

class MagaDataSet(DataSet):
    def __init__(self,caminho):
        super().__init__(caminho,delimiter=',')

class NetsDataSet(DataSet):
    def __init__(self, caminho):
        super().__init__(caminho,delimiter=',')