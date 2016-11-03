import os
from importlib.machinery import SourceFileLoader
thispath = os.path.dirname(os.path.abspath(__file__))
Wrapper = SourceFileLoader("Wrapper", os.path.join(thispath, "Wrapper7.py")).load_module()
from zashel.utils import make_daemon
import datetime
import comtypes.client

class App(Wrapper.App):
    def __init__(self):
        super().__init__()

    def login(self, username, password):
        super().login(instance="172.16.70.146", username=username, password=password)

    def get_cartera(self, archivo=os.path.abspath(os.environ["HOMEPATH"])+r"\documents\jazztelextract.csv"):
        campaign = "ct_emi_cobro_tww"
        datos = self.execute("select * from {} as jazz")
        final = ";".join(datos.columns) + ";\n"
        final.replace("jazz.", "")
        for dato in datos:
            for column in datos.columns:
                final+=dato.__getattr__(column)+";"
            final+=";\n"
        with open(archivo, "w") as jazztel:
            jazztel.write(final)
        print("{} DONE!".format(archivo))
