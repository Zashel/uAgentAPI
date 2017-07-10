import os
from importlib.machinery import SourceFileLoader
thispath = os.path.dirname(os.path.abspath(__file__))
Wrapper = SourceFileLoader("Wrapper", os.path.join(thispath, "Wrapper.py")).load_module()
from zashel.utils import buscar_unidad, make_daemon

from zashel import modular
import datetime
import comtypes.client
import re
import socket
import json
import base64
import hashlib
import io
import struct


 #########################
#                        #
# Principal Application  #
#                        #
#########################

class App(Wrapper.App):
    def __init__(self, path=None):
        super().__init__(path, pathclass=Path)
        self._modules = ScriptModuleWrapper(self)

    @property
    def modules(self):
        return self._modules

    #############
    #            #
    # Methods:   #
    # Para Todos #
    #            # 
     #############

    @staticmethod
    def a_long_time_ago(days=5):
        return datetime.date.today()-datetime.timedelta(days=days)

    def insert_into_dncl(self, number, day=None):
        number = str(number)
        ph_number = "+34{}".format(number)
        if not day:
            day = datetime.datetime.today()+datetime.timedelta(90)
        else:
            day = datetime.datetime.strptime(day, "%Y-%m-%d")
        day = day.strftime("%Y-%m-%d 00:00:00")
        sql = "select code from dncl_entry where ph_number=? and dncl=2"
        data = self.execute(sql, (ph_number, ))        
        if data.count==0:
            sql = "select max(code) as max from dncl_entry"
            dato = self.execute(sql)
            sql = "insert into dncl_entry (code, ph_number, original_number, " 
            sql += "expire_date, dncl) values (?, ?, ?, ?, '2')"
            try:
                self.execute(sql, (int(dato[0].max)+1, ph_number, number, day))
                return True
            except:
                raise
        else:
            sql = "update dncl_entry set expire_date = ? where code = ?"
            try:
                self.execute(sql, (day, data[0].code))
                return True
            except:
                return False

    def get_dncl(self):
        sql = "select original_number as phone from dncl_entry where dncl = 2 and (convert(date, expire_date) >= getdate() or expire_date is null);"
        phones = App.execute(self, sql)
        final = list()
        for phone in phones:
            try:
                final.append(int(phone.phone))
            except:
                pass
                
        return final

    def read_transferencias(self, archivo): #Solo texto, por favor.
        final = dict()
        with open(archivo, "r") as pagos:
            cabeceras = dict()
            for row_index, datos in enumerate(pagos):
                datos = datos.strip("\n").split("\t")
                if row_index == 0:
                    cabeceras.update({
                            "idtransfer": datos.index("referencia1"),
                            "referencia": datos.index("referencia2"),
                            "numeros": datos.index("num_documento"),
                            "importe": datos.index("importe"),
                            "comentario": datos.index("comentario"),
                            "nif": datos.index("nif"),
                            "nombre": datos.index("nombre"),
                            "f_operacion": datos.index("fecha_operacion"),
                            "f_valor": datos.index("fecha_valor")
                            })
                else:
                    pago = dict()
                    for item in cabeceras: #Esto es duro
                        pago[item] = datos[cabeceras[item]]
                    strings = list()
                    strings.append(pago["numeros"])
                    strings.append("{} {}".format(pago["referencia"], pago["comentario"]))
                    strings.append("{}{}".format(pago["referencia"], pago["comentario"]))
                    strings.append("{} {}".format(pago["comentario"], pago["referencia"]))
                    strings.append("{}{}".format(pago["comentario"], pago["referencia"]))
                    strings.append("{}{}".format(pago["nif"], pago["nombre"]))
                    strings.append("{} {}".format(pago["nif"], pago["nombre"]))
                    indices = set()
                    for string in strings:
                        for restring in (
                                string, 
                                string.replace(".", ""), 
                                string.replace("-", ""), 
                                string.replace(" ", "")
                                ):
                            for nif in re.findall(r"[DNI]?[ ]?([XYZ]?[0-9]{5,8}[A-Z]{0,1})[ ]?", restring.upper()):
                                indices.add(nif)
                            for cif in re.findall(r"[A-Z]{1}[0-9]{8}", restring.upper()):
                                indices.add(cif)
                            for tel in re.findall(r"\+34[0-9]{9}|[0-9]{9}", restring.upper()):
                                indices.add(tel.strip("+34"))
                            for cc in re.findall(r"1.[0-9]{8}", restring.upper()):
                                indices.add(cc)
                    for indice in indices:
                        if not indice in final:
                            final[indice] = list()
                        final[indice].append(pago)
        return final
                        

    def alinear_datos(self):
        sql = """update dir_recobros_orange set call_status = load.call_status, amount_unpaid = load.amount_unpaid
from dir_recobros_orange as dir
inner join contact_profile as cp 
on dir.easycode = cp.code inner join ext_esma01.dbo.recobros_orange_load_contacts as load 
on cp.unique_id = load.contact_profile_unique_id 
where (dir.call_status <> load.call_status or
dir.amount_unpaid <> load.amount_unpaid) and 
dir.dat_dead_line >= convert(date, getdate())"""
        self.execute(sql)
        return True

    def get_contact_tries(self, *, servicio="Móvil", segmento=str(), fecha=None, fecha_fin=None, campana=None, separator=","):
        lista_campanas = (2, 4, 5, 7, 8)
        if not fecha:
            fecha = (datetime.datetime.today() - datetime.timedelta(60)).strftime("%Y-%m-%d")
        if campana and campana == "IVR": lista_campanas = (5, 5, 5, 5, 5)
        elif campana and "RES" in campana: 
            if servicio.lower() == "móvil": lista_campanas = (2, 2, 2, 2, 2)
            else: lista_campanas = (8, 8, 8, 8, 8)
        elif campana and "EMP" in campana: 
            if servicio.lower() == "móvil": lista_campanas = (4, 4, 4, 4, 4)
            else: lista_campanas = (7, 7, 7, 7, 7)
        if not fecha_fin: fecha_fin = fecha 
        if servicio.lower() == "móvil":
            segmento = App.verify_segmento_movil(segmento)
            sql = """select top(100)percent dir.easycode, dir.cod_client as cod_client, dir.typ_business_segment, dir.dat_unpaid_invoice as fecha, dir.amount_start, dir.amount_unpaid, ah.event_action, ah.active_campaign, convert(date, ah.event_moment) as f_moment, count(ah.event_action) as totales from
dir_recobros_orange as dir left join activity_history as ah
on dir.easycode = ah.contact_profile
where dir.dat_unpaid_invoice >= ? and dir.dat_unpaid_invoice <= ? and dir.typ_business_segment like ? and (ah.active_campaign in (?, ?, ?, ?, ?) or ah.active_campaign is null) and (ah.event_action in (4, 6) or ah.event_action is null)
group by easycode, dir.cod_client, dir.typ_business_segment, dir.dat_unpaid_invoice, dir.amount_start, dir.amount_unpaid, ah.event_action, ah.active_campaign, convert(date, ah.event_moment)"""
        elif servicio.lower() == "fijo":
            segmento = App.verify_segmento_fijo(segmento)
            fecha = (datetime.datetime.strptime(fecha, "%Y-%m-%d")+datetime.timedelta(60)).strftime("%Y-%m-%d")
            fecha_fin = (datetime.datetime.strptime(fecha_fin, "%Y-%m-%d")+datetime.timedelta(60)).strftime("%Y-%m-%d")
            sql = """select top(100)percent dir.easycode, dir.cod_client as cod_client, dir.typ_business_segment, dir.lote as fecha, dir.amount_start, dir.amount_unpaid, ah.event_action, ah.active_campaign, convert(date, ah.event_moment) as f_moment, count(ah.event_action) as totales from
dir_recobros_orange as dir left join activity_history as ah
on dir.easycode = ah.contact_profile
where dir.dat_dead_line >= ? and dir.dat_dead_line <= ? and dir.typ_business_segment like ? and (ah.active_campaign in (?, ?, ?, ?, ?) or ah.active_campaign is null) and (ah.event_action in (4, 6) or ah.event_action is null)
group by easycode, dir.cod_client, dir.typ_business_segment, dir.lote, dir.amount_start, dir.amount_unpaid, ah.event_action, ah.active_campaign, convert(date, ah.event_moment)"""
        else:
            return None
        print((fecha, fecha_fin, segmento, lista_campanas[0], lista_campanas[1], lista_campanas[2], lista_campanas[3], lista_campanas[4]))
        datos = self.execute(sql, (fecha, fecha_fin, segmento, lista_campanas[0], lista_campanas[1], lista_campanas[2], lista_campanas[3], lista_campanas[4]))
        final = dict()
        print(str(datos.count))
        for dato in datos:
            if not dato.cod_client in final:
                final[dato.cod_client] = dict()
            nombre = "failed" # event_action == '4'
            if dato.event_action == '6': nombre = "handled"
            if servicio.lower() == "móvil":
                fecha = datetime.datetime.strptime(dato.fecha, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
            else:
                fecha = dato.fecha
            if not fecha in final[dato.cod_client]:
                final[dato.cod_client][fecha] = dict()
            try:
                event_moment = datetime.datetime.strptime(dato.f_moment, "%Y-%m-%d %H:%M:%S")
            except:
                event_moment = ""
            if not event_moment in final[dato.cod_client][fecha]:
                final[dato.cod_client][fecha][event_moment] = dict()
            final[dato.cod_client][fecha][event_moment].update({
                    "easycode": dato.easycode,
                    "segmento": dato.typ_business_segment,
                    "importe_inicial": dato.amount_start != "" and round(float(dato.amount_start.replace(",", ".")), 2) or "",
                    "importe_impagado": dato.amount_unpaid != "" and round(float(dato.amount_unpaid.replace(",", ".")), 2) or 0,
                    "al_corriente": dato.amount_unpaid==".00",
                    nombre: int(dato.totales),
                    "active_campaign": dato.active_campaign != "" and int(dato.active_campaign) or -1,
                    })
        finalstr = servicio.lower() == "móvil" and "EasyCode;Segmento;CC;Fecha_Factura;Importe_Inicial;Importe_Impagado;Fecha;Al_Corriente;Totales;Atendidos;Fallidos;Campaña\n" or "EasyCode;Segmento;ExternalID;Lote;Importe_Inicial;Importe_Impagado;Fecha;Al_Corriente;Totales;Atendidos;Fallidos;Campaña\n"
        campanas = {-1:"", 2: "OB", 4: "OB", 5: "IVR", 7: "OB", 8: "OB"}
        for cc in final:
            for fecha in final[cc]:
                for moment in final[cc][fecha]:
                    dato = final[cc][fecha][moment]
                    failed = handled = 0
                    if "failed" in dato:
                        failed = dato["failed"]
                    if "handled" in dato:
                        handled = dato["handled"]
                    finalstr += ";".join((
                            str(dato["easycode"]),
                            dato["segmento"], 
                            cc, 
                            fecha, 
                            str(dato["importe_inicial"]).replace(".", separator), 
                            str(dato["importe_impagado"]).replace(".", separator),
                            str(moment),
                            str(dato["al_corriente"]), 
                            str(failed+handled),
                            str(handled),
                            str(failed),
                            campanas[dato["active_campaign"]]
                            ))+"\n"
        final["texto"] = finalstr
        return final

    def riesgo(self, f_ini, f_fin, archivo, cabeceras=False):
        sql = """select top(100)percent 
    dir.easycode as EasyCode,
    dir.cod_client as ExtID,
    dir.customer_id as NIF,
    dir.dat_unpaid_invoice as Ciclo,
    convert(date, dir.dat_load) as Fecha_Inicio,
    ah.active_campaign as Campana,
    dir.amount_single as Importe_Factura,
    dir.amount_unpaid as Importe_Impagado,
    dir.amount_single - dir.amount_unpaid as Importe_Pagado,
    Pago = 
        case 
            when dir.amount_unpaid = 0 then 'Total'
            when dir.amount_unpaid < dir.amount_single then 'Parcial'
            when dir.amount_unpaid = dir.amount_single then 'Nulo'
        end,
    Estado_Llamadas = 
        case ah.event_action
            when 6 then 'Atendidas'
            when 4 then 'Fallidas'
            else 'Sin Llamadas'
        end,
    convert(date, ah.event_moment) as Fecha, 
    count(ah.event_action) as Total_Llamadas 

from 
    dir_recobros_orange as dir 
        left join activity_history as ah

on 
    dir.easycode = ah.contact_profile

where
    ah.event_moment >= ? and
    ah.event_moment <=  ? and 
    dir.typ_business_segment like 'RES_%' and
    (ah.active_campaign in (2, 4, 5, 7, 8) or 
            ah.active_campaign is null) and 
    (ah.event_action in (4, 6) or 
            ah.event_action is null)

group by 
    easycode, 
    dir.cod_client,
    dir.customer_id, 
    dir.dat_unpaid_invoice, 
    convert(date, dir.dat_load),
    ah.active_campaign,
    dir.amount_single, 
    dir.amount_unpaid,
    dir.amount_single - dir.amount_unpaid,
    case 
        when dir.amount_unpaid = 0 then 'Total'
        when dir.amount_unpaid < dir.amount_single then 'Parcial'
        when dir.amount_unpaid = dir.amount_single then 'Nulo'
    end,
    case ah.event_action
        when 6 then 'Atendidas'
        when 4 then 'Fallidas'
        else 'Sin Llamadas'
    end,
    convert(date, ah.event_moment)"""
        datos = self.execute(sql, (f_ini, f_fin))
        if cabeceras:
            final = ";".join(datos.columns)+";\n"
        else:
            final = str()
        for dato in datos:
            for column in datos.columns:
                final += dato.__getattr__(column)+";"
            final += "\n"

        with open(archivo, "a") as riesgo:
            riesgo.write(final)


    #############    #### #     #  ###
    #            #  #     #     # #   #
    # Methods:   #  ###   #     # #   #
    #       Fijo #  #     # #   # #   #
    #            #  #     #  ###   ###   
     #############                     

    @staticmethod
    def verify_dat_dead_line(dat_dead_line=None): #TODO: Verificar dato dado
        if not dat_dead_line:
            dat_dead_line = datetime.datetime.today()
            dat_dead_line = dat_dead_line.strftime("%Y-%m-%d")
        return dat_dead_line
        
    @staticmethod
    def verify_segmento_fijo(segmento=str()):
        if segmento and (
                "res" in segmento.lower() or
                "home" in segmento.lower() or
                "par" in segmento.lower() 
                ):
            segmento = "RES_FIJ"
        elif segmento and (
                "emp" in segmento.lower() or
                "aut" in segmento.lower() or
                "pyme" in segmento.lower()
                ):
            segmento = "EMP_FIJ"            
        else:
            segmento = "FIJ"
        return segmento
        
    def get_dni_fijo_pendiente(self, dat_dead_line=None, *, segmento=str()):
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        segmento = App.verify_segmento_fijo(segmento)
        sql = "select distinct customer_id, lote from dir_recobros_orange "
        sql += "where typ_business_segment like ? and dat_dead_line >= ? "
        sql += "and call_status = 1 order by lote asc;"
        return App.execute(self, sql, (segmento, dat_dead_line))
        
    def get_carteras_fijo(self, dat_dead_line=None, *, segmento=str()):
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        segmento = App.verify_segmento_fijo(segmento)
        sql = "select * from dir_recobros_orange inner join contact_profile "
        sql += "on easycode=code "
        sql += "where typ_business_segment like ? and dat_dead_line >= ? "
        sql += "and contact_profile.unique_id like '%F%';"
        return App.execute(self, sql, (segmento, dat_dead_line))

    def get_carteras_fijo_directorio(self, dat_dead_line=None, *, segmento=str(), final=None):
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        final = App.verify_dat_dead_line(final)
        segmento = App.verify_segmento_fijo(segmento)
        sql = "select * from dir_recobros_orange "
        sql += "where typ_business_segment like ? and dat_dead_line >= ? "
        sql += "and dat_dead_line <= ?"
        return App.execute(self, sql, (segmento, dat_dead_line, final))

    def get_clientes_fijo_pendientes(self, dat_dead_line=None, *, segmento=str()):
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        segmento = App.verify_segmento_fijo(segmento)
        if segmento == "FIJ": raise AttributeError ("Se ha de especificar el segmento.")
        sql = "select dir.cod_father as Padre, dir.cod_son as Hijo, dir.customer_id as NIF, "
        sql += "cp.first as Nombre, dir.cod_invoice as Numero_Factura, " 
        sql += "convert(date, dir.dat_unpaid_invoice) as Fecha_Factura, "
        sql += "dir.amount_unpaid as Balance, ext.tfno_titular as phone, "
        sql += "dir.lote as Lote, dir.call_status as No_Action from (dir_recobros_orange as dir "
        sql += "inner join ext_esma01.dbo.recobros_orange_{}o_cartera as ext ".format(segmento)
        sql += "on dir.cod_invoice = ext.num_factu) "
        sql += "inner join contact_profile as cp on dir.easycode = cp.code where typ_business_segment like ? "
        sql += "and dat_dead_line >= ? and call_status in (1, 5) and cp.unique_id like '%F%' "
        sql += "order by Lote asc, Balance desc;"
        try:
            return App.execute(self, sql, (segmento, dat_dead_line))
        except:
            print(sql)
            raise

    def get_compromisos_pago_fijo(self, dat_dead_line=None, *, segmento=str()):
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        segmento = App.verify_segmento_fijo(segmento)
        sql = "select distinct customer_id from dir_recobros_orange "
        sql += "where typ_business_segment like ? and dat_dead_line >= ? "
        sql += "and last_outcome in (102, 4010, 4020, 4030) and contact_date >= ? and call_status in (1, 5);"
        return App.execute(self, sql, (segmento, dat_dead_line, App.a_long_time_ago()))

    def get_incumples_fijo(self, dat_dead_line=None, *, segmento=str()):
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        segmento = App.verify_segmento_fijo(segmento)
        sql = "select distinct customer_id from dir_recobros_orange "
        sql += "where typ_business_segment like ? and dat_dead_line >= ? "
        sql += "and last_outcome in (102, 4010, 4020, 4030) and contact_date <= ? and call_status = 1;"
        return App.execute(self, sql, (segmento, dat_dead_line, App.a_long_time_ago()))

    def get_lista_rpv_ia_fijo_emp(self):
        sql = "select customer_id from dir_recobros_orange "
        sql += "inner join activity on contact_profile = easycode "
        sql += "where act_list = 116" #Lista RPV_IA -> Si cambia hay que darles por saco.
        query = App.execute(self, sql)
        total = set([data.customer_id for data in query])
        return list(total)
 
    def get_negativas_fijo(self, dat_dead_line=None, *, segmento=str()):
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        segmento = App.verify_segmento_fijo(segmento)
        sql = "select distinct customer_id from dir_recobros_orange "
        sql += "where typ_business_segment like ? and dat_dead_line >= ? "
        sql += "and last_outcome in (103, 2000, 2010) and call_status = 1;"
        return App.execute(self, sql, (segmento, dat_dead_line))
        
    def get_moviles_fijo(self, ruta):
        print("Ruta Móviles Fijo: {}".format(ruta))
        files = list()
        for f in os.listdir(ruta):
            try:
                if datetime.datetime.strptime(f[-10:-4], "%d%m%y")>=datetime.datetime.today()-datetime.timedelta(60) and os.path.isfile(os.path.join(ruta, f)):
                    files.append(f)
            except:
                pass
        telefonos = dict()
        excel = comtypes.client.CreateObject("Excel.Application")
        for file_ in files:
            if "tlfs_moviles" in file_.lower() and "xls" in file_[-5:].lower():
                telefonos.update(self.read_moviles_fijos(os.path.join(ruta, file_), excelapp=excel)) 
        excel.Quit()
        return telefonos

    def get_fijos_fijo(self, ruta):
        print("Ruta Fijos Fijo: {}".format(ruta))
        files = list()
        for f in os.listdir(ruta):
            try:
                if datetime.datetime.strptime(f[-10:-4], "%d%m%y")>=datetime.datetime.today()-datetime.timedelta(60) and os.path.isfile(os.path.join(ruta, f)):
                    files.append(f)
            except:
                pass
        telefonos = dict()
        excel = comtypes.client.CreateObject("Excel.Application")
        for file_ in files:
            if "tlfs_fijos" in file_.lower() and "xls" in file_[-5:].lower():
                telefonos.update(self.read_fijos_fijos(os.path.join(ruta, file_), excelapp=excel)) 
        excel.Quit()
        return telefonos

    def get_resumen_carteras_fijo(self, dat_dead_line=None, *, segmento=str()):
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        segmento = App.verify_segmento_fijo(segmento)
        sql = "select lote, call_status, "
        sql += "sum(amount_unpaid) as importe, "
        sql += "min(dat_unpaid_invoice) as factura_antigua "
        sql += "from dir_recobros_orange "
        sql += "where dat_dead_line >= ? and typ_business_segment like ? "
        sql += "group by lote, call_status "
        sql += "order by lote, call_status;"
        return App.execute(self, sql, (dat_dead_line, segmento))
        
    def read_moviles_fijos(self, ruta, *, excelapp=None):
        if not excelapp:
            excel = comtypes.client.CreateObject("Excel.Application")
        else:
            excel = excelapp
        base, file_ = os.path.split(ruta)
        telefonos = dict()
        if "tlfs_moviles" in file_.lower() and "xls" in file_[-5:].lower():
            libro = excel.Workbooks.Open(ruta)
            hoja1 = libro.Worksheets(1)
            ini = 2
            while hoja1.Cells(ini, 2).value():
                nif = hoja1.Cells(ini, 2).value()
                tel = int(hoja1.Cells(ini, 3).value())
                ini += 1
                if not nif in telefonos:
                    telefonos[nif] = set()
                telefonos[nif].add(tel)
            libro.Close(SaveChanges=False)
        if not excelapp:
            excel.Quit()
        return telefonos

    def read_fijos_fijos(self, ruta, *, excelapp=None):
        if not excelapp:
            excel = comtypes.client.CreateObject("Excel.Application")
        else:
            excel = excelapp
        base, file_ = os.path.split(ruta)
        telefonos = dict()
        if "tlfs_fijos" in file_.lower() and "xls" in file_[-5:].lower():
            libro = excel.Workbooks.Open(ruta)
            hoja1 = libro.Worksheets(1)
            ini = 2
            while hoja1.Cells(ini, 1).value():
                try:
                    nif = hoja1.Cells(ini, 1).value()
                    tel = int(hoja1.Cells(ini, 2).value())
                    ini += 1
                    if not nif in telefonos:
                        telefonos[nif] = set()
                    telefonos[nif].add(tel)
                except:
                    ini += 1
                    continue
            libro.Close(SaveChanges=False)
        if not excelapp:
            excel.Quit()
        return telefonos

    #############   #   #  ###  #   # # #
    #            #  ## ## #   # #   # # #
    # Methods:   #  # # # #   # #   # # #
    #      Móvil #  #   # #   #  # #  # #
    #            #  #   #  ###    #   #  ####
     #############

    @staticmethod
    def verify_segmento_movil(segmento=str()):
        if segmento and (
                "res" in segmento.lower() or
                "home" in segmento.lower() or
                "par" in segmento.lower() 
                ):
            segmento = "RES_MOV"
        elif segmento and (
                "emp" in segmento.lower() or
                "aut" in segmento.lower() or
                "pyme" in segmento.lower()
                ):
            segmento = "EMP_MOV"            
        else:
            segmento = "MOV"
        return segmento

    def calculate_pagos_movil_en_gestion(self, file_, *, segmento=str()):
        segmento = App.verify_segmento_movil(segmento)
        clientes = self.get_idopen_clientes_en_gestion(segmento = segmento)
        pagos = self.read_exportacion_pagos_open(file_)
        total = dict()
        for cliente in pagos:
            if cliente in clientes:
                for factura in pagos[cliente]:
                    if (factura not in total and
                            factura in clientes[cliente]):
                        total[factura] = [int(), ]
                    if factura in clientes[cliente]:
                        total[factura][0] += pagos[cliente][factura]
        for factura in total:
            dato = str(total[factura][0])            
            total[factura][0] = "{},{} €".format(len(dato)>2 and dato[:-2] or "0", dato[-2:])
        return total

    def get_idopen_clientes_en_gestion(self, dat_dead_line=None, *, segmento=str(), bscs=None):
        segmento = App.verify_segmento_movil(segmento)
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        sql = "select id_open as IdOpen, convert(char, dat_unpaid_invoice, 103) as FechaFactura, "
        sql += "amount_unpaid as Importe, cod_bscs as Bscs "
        sql += "from dir_recobros_orange as dir "
        sql += "where typ_business_segment like ? and dat_dead_line >= ? "
        sql += "and call_status = 1 "
        sql += "order by dat_dead_line asc;"
        datos = App.execute(self, sql, (segmento, dat_dead_line))
        final = dict()
        for index, dato in enumerate(datos):
            if dato.idopen != "":
                if bscs is None or dato.bscs in bscs:
                    idopen = int(dato.idopen)
                    if idopen not in final:
                        final[idopen] = dict()
                    if dato.fechafactura not in final[idopen]:
                        final[idopen][dato.fechafactura] = int()
                    final[idopen][dato.fechafactura] = round( float(dato.importe)*100)
        return final
        
    def get_pagos_sobre_factura_posterior(self, dat_dead_line=None, *, segmento=str()):
        segmento = App.verify_segmento_movil(segmento)
        dat_dead_line = App.verify_dat_dead_line(dat_dead_line)
        sql = "select rc1.cod_client, rc1.dat_unpaid_invoice, rc1.amount_unpaid, rc2.dat_unpaid_invoice, "
        sql += "(rc2.amount_single - rc2.amount_unpaid) as rc2_amount_paid from dir_recobros_orange as rc1 "
        sql += "inner join dir_recobros_orange as rc2 on rc1.cod_client = rc2.cod_client and "
        sql += "rc1.dat_unpaid_invoice < rc2.dat_unpaid_invoice where rc1.call_status = 1 and "
        sql += "(rc2.amount_single - rc2.amount_unpaid) > 0 and rc1.typ_business_segment like ? and "
        sql += "rc1.dat_dead_line > ? order by rc1.dat_dead_line asc;"
        return App.execute(self, sql, (segmento, dat_dead_line))

    def read_exportacion_pagos_open(self, file_):
        datos = dict()
        if isinstance(file_, list):
            for index, pago in enumerate(file_):
                if index > 0:
                    id_open = int(pago["external_id"])
                    f_fact = pago["f_fact"].strftime("%d/%m/%Y")
                    try:
                        importe = round(float(pago["imp_cobrado"].replace(",", ".")) * 100)
                    except AttributeError:
                        importe = round(float(pago["imp_cobrado"]) * 100)
                    if id_open not in datos:
                        datos[id_open] = dict()
                    if f_fact not in datos[id_open]:
                        datos[id_open][f_fact] = int()
                    datos[id_open][f_fact] += importe
        else:
            with open(file_, "r") as pagos:
                for index, pago in enumerate(pagos):
                    if index > 0:
                        fila = pago.split("\t")
                        id_open = int(fila[1])
                        f_fact = fila[12][:10]
                        importe = round(float(fila[2].replace(",", "."))*100)
                        if id_open not in datos:
                            datos[id_open] = dict()
                        if f_fact not in datos[id_open]:
                            datos[id_open][f_fact] = int()
                        datos[id_open][f_fact] += importe
        return datos

    def get_dolphin_from_files(self, first_date, *, segmento=str()):
        first_date = datetime.datetime(first_date.year, first_date.month, first_date.day)
        if "res" in segmento.lower():
            path = self.config.path.dataloading.residencial_movil_info_adicional_imported
        elif "emp" in segmento.lower():
            path = self.config.path.dataloading.empresa_movil_info_adicional_imported
        lsdir = os.listdir(path)
        lsdir.sort()
        informacion_adicional = dict()
        for info in lsdir:
            f_file = datetime.datetime.strptime(info[-12:-4], "%Y%m%d")
            if not first_date or (f_file >= first_date):
                with open(os.path.join(path, info), "r") as file_:
                    for row in file_:
                        row = row.replace("\n", "")
                        data = row.split("|")
                        cc = data[1]
                        dolphin = data[22]
                        if not cc in informacion_adicional:
                            informacion_adicional[cc] = dict()
                        if "s" in dolphin.lower() or not f_file in informacion_adicional[cc]:
                            informacion_adicional[cc][f_file] = dolphin
        return informacion_adicional

    def get_max_outcome_for_contact(self, first_date, *, segmento=str()):
        segmento = App.verify_segmento_movil(segmento)
        codes = {"RES_MOV": "2", "EMP_MOV": "4"}
        query = """
select CC, FechaFactura, Outcome, CallStatus, Dolphin, DateControl, DeadLine from
(select
CC,
FechaFactura,
Outcome =
case
when Estado = '0' then
case
when MaxOutcome between 8 and 10 then 'AL CORRIENTE - DEJO AVISO'
when MaxOutcome between 11 and 20 then 'AL CORRIENTE - COMPROMISO PAGO'
when MaxOutcome between 4000 and 5999 then 'AL CORRIENTE - COMPROMISO PAGO'
when MaxOutcome between 3000 and 3999 then 'AL CORRIENTE - RECLAMACION'
when MaxOutcome between 2000 and 2999 then 'AL CORRIENTE - NEGATIVA'
when MaxOutcome in (150, 910, 920) then 'AL CORRIENTE - DEJO AVISO'
when DialRule <> -1 or
Moment is not null or
LastAgent is not null or
LastOutcome is not null or
MaxOutcome is not null or
NTries > 0 or
Importe >= 400 then 'AL CORRIENTE - NO CONTACTO'
else 'AL CORRIENTE - SIN GESTION'
end
when Estado = '3' then 'NO CONTACTO'
when Estado = '1' then
case
when MaxOutcome between 8 and 10 then 'DEJO AVISO'
when MaxOutcome between 11 and 20 then 'COMPROMISO PAGO'
when MaxOutcome between 4000 and 5999 then 'COMPROMISO PAGO'
when MaxOutcome between 3000 and 3999 then 'RECLAMACION'
when MaxOutcome between 2000 and 2999 then 'NEGATIVA'
when MaxOutcome in (150, 910, 920) then 'DEJO AVISO'
when DialRule <> -1 or
Moment is not null or
LastAgent is not null or
LastOutcome is not null or
MaxOutcome is not null or
NTries > 0 or
Importe >= 400 then 'NO CONTACTO'
else 'SIN LLAMADA'
end
else 'NO CONTACTO'
end,
CallStatus, Dolphin, DateControl, DeadLine
from
(select distinct top(100)percent max(tsev.value) as MaxOutcome, cp.unique_id as EasyCode,
convert(char, dir.dat_unpaid_invoice, 112) as FechaFactura, max(activity.moment) as Moment,
min(dir.call_status) as Estado, max(activity.dial_rule) as DialRule, min(dir.last_agent) as LastAgent,
max(activity.ntries_auto)+max(activity.ntries_manual) as NTries, max(dir.amount_unpaid) as Importe,
max(dir.last_outcome) as LastOutcome, dir.dolphin as Dolphin, dir.cod_bscs as CC, load.call_status as CallStatus,
load.dat_update_control as DateControl, dir.dat_dead_line as DeadLine 
from ph_table_schema_enum_value as tsev right join
(ao_recobros_orange as aoro right join
(activity_outcome as ao right join
(contact_profile as cp right join
(activity right join
(ext_esma01.dbo.recobros_orange_load_contacts as load right join dir_recobros_orange as dir
on load.cod_invoice = dir.cod_invoice)
on dir.easycode = activity.contact_profile
)
on dir.easycode = cp.code
)
on ao.contact_profile = cp.code
)
on aoro.easycode = ao.code
)
on tsev.code = aoro.outcome_value
where activity.campaign = {} and
dir.typ_business_segment = '{}'
and (ao.start_time >= dateadd(day, -60, getdate()) or ao.start_time is null)
group by cp.unique_id, convert(char, dir.dat_unpaid_invoice, 112), dir.dolphin, load.call_status, load.dat_update_control, dir.dat_dead_line, dir.cod_bscs
) a )a """.format(codes[segmento], segmento)
        return App.execute(self, query)
        
 ###########################
#                          #
# Path Wrapper for config  #
#                          #
###########################

class Path(Wrapper.Path):
    def __init__(self, config, base=os.environ["HOMEPATH"]):
        super().__init__(config, base, "CobrosOrange")

    @property
    def dataloading(self):
        if not "dataloading" in self._config:
            self._config["dataloading"] = {
                    "imported_folder": "IMPORTED",
                    "to_import_folder": "TO_IMPORT",
                    "duplicate_folder": "DUPLICATE_FILES",
                    "empresa_fijo_cartera": r"EMPRESA_FIJO\CARTERA",
                    "empresa_fijo_cheques": r"EMPRESA_FIJO\CHEQUES_TRANSFERENCIA",
                    "empresa_fijo_cobros": r"EMPRESA_FIJO\COBROS",
                    "empresa_fijo_fraudes": r"EMPRESA_FIJO\FRAUDES",
                    "empresa_fijo_no_action": r"EMPRESA_FIJO\NO_ACTION",
                    "empresa_fijo_retirada": r"EMPRESA_FIJO\RETIRADA_CARTERA",
                    "empresa_fijo_fijos": r"EMPRESA_FIJO\TLFS_FIJOS",
                    "empresa_fijo_moviles": r"EMPRESA_FIJO\TLFS_MOVILES",
                    "residencial_fijo_cartera": r"RESIDENCIAL_FIJO\CARTERA",
                    "residencial_fijo_cheques": r"RESIDENCIAL_FIJO\CHEQUES_TRANSFERENCIA",
                    "residencial_fijo_cobros": r"RESIDENCIAL_FIJO\COBROS",
                    "residencial_fijo_fraudes": r"RESIDENCIAL_FIJO\FRAUDES",
                    "residencial_fijo_no_action": r"RESIDENCIAL_FIJO\NO_ACTION",
                    "residencial_fijo_retirada": r"RESIDENCIAL_FIJO\RETIRADA_CARTERA",
                    "residencial_fijo_fijos": r"RESIDENCIAL_FIJO\TLFS_FIJOS",
                    "residencial_fijo_moviles": r"RESIDENCIAL_FIJO\TLFS_MOVILES",
                    "empresa_movil_canguro": r"EMPRESA_MOVIL\CANGURO",
                    "empresa_movil_dev": r"EMPRESA_MOVIL\DEV_EMP",
                    "empresa_movil_fraudes": r"EMPRESA_MOVIL\FRAUDES",
                    "empresa_movil_info_adicional": r"EMPRESA_MOVIL\INFO_ADICIONAL",
                    "empresa_movil_notas": r"EMPRESA_MOVIL\NOTAS_DE_ABONO",
                    "empresa_movil_pagos": r"EMPRESA_MOVIL\PAGAMENTOS",
                    "empresa_movil_vips": r"RESIDENCIAL_MOVIL\VIPS",
                    "residencial_movil_canguro": r"RESIDENCIAL_MOVIL\CANGURO",
                    "residencial_movil_dev": r"RESIDENCIAL_MOVIL\DEV_RES",
                    "residencial_movil_fraudes": r"RESIDENCIAL_MOVIL\FRAUDES",
                    "residencial_movil_info_adicional": r"RESIDENCIAL_MOVIL\INFO_ADICIONAL",
                    "residencial_movil_notas": r"RESIDENCIAL_MOVIL\NOTAS_DE_ABONO",
                    "residencial_movil_pagos": r"RESIDENCIAL_MOVIL\PAGAMENTOS",
                    "residencial_movil_vips": r"RESIDENCIAL_MOVIL\VIPS",
                    "fin_gestiones": "FIN_GESTIONES",
                    }
            self._config.save()
        class DataLoading():
            def __init__(self, config):
                self.config = config

            def __dir__(self):
                _dir = object.__dir__(self)
                data = [key for key in self.config["dataloading"]]
                _dir.extend(data)
                return _dir

            def __getattr__(self, key):
                dato = str()
                if not "_folder" in key and (
                        "_imported" in key or 
                        "_to_import" in key or 
                        "_duplicate" in key
                        ):
                    subkey, folder = re.findall(r"([\w_ ]+)(_imported|_to_import|_duplicate)", key)[0]
                    if not "_folder" in subkey and subkey in self.config["dataloading"]:
                        dato = r"{}\{}".format(self.config["dataloading"][subkey], self.config["dataloading"][r"{}_folder".format(folder[1:])])
                else:
                    if not "_folder" in key and key in self.config["dataloading"]:
                        dato = self.config["dataloading"][key]
                return buscar_unidad(dato)
        return DataLoading(self._config)

    @property
    def modular(self):
        if not "modular" in self._config:
            self._config["modular"] = dict()
        if not "local-script" in self._config["modular"]:
            modular_path = r"{}\modular".format(self.base)        
            self._config["modular"]["local-script"] = modular_path
            if not os.path.exists(modular_path):
                os.mkdir(modular_path)
        if not "share-script" in self._config["modular"]:
            self._config["modular"]["share-script"] = "OCA\modules"
        
        return EnUnidad(self._config, "modular")

    @property
    def flask(self):
        if not "flask" in self._config:
            self._config["flask"] = dict()
        if not "templates" in self._config["flask"]:
            self._config["flask"]["templates"] = r"OCA\templates"
        return EnUnidad(self._config, "flask") 

class EnUnidad():
    def __init__(self, config, key):
        self.config = config
        self.key = key

    def __iter__(self):
        for data in self.config[self.key]:
            yield data

    def __getattr__(self, key):
        if key in self.config[self.key]:
            return buscar_unidad(self.config[self.key][key])
        else:
            return None

    def __getitem__(self, key):
        if key in self.config[self.key]:
            try:
                return buscar_unidad(self.config[self.key][key])
            except:
                return None
        else:
            return None
            
        
 #######################
#                      #
# Modular Module Base  #
#                      #
#######################

class ScriptModuleWrapper(object):
    def __init__(self, app):
        self._app = app
        self._sock = socket.socket()
        self.port = None
        for port in range(5050, 5060):
            try:
                self.sock.bind(("",port))
                self.listen()
                self.conn, self.addr = None, None
                self.port = port
                break
            except:
                pass
        
    def __del__(self):
        self.sock.close()
        
    @property
    def app(self):
        return self._app

    @property
    def sock(self):
        return self._sock
        
    @make_daemon
    def listen(self):
        self.sock.listen(10)
        while True:
            self.conn, self.addr = self.sock.accept()
            response = self.conn.recv(1024)
            response = response.decode("utf-8").split("\r\n")
            self.headers = dict()  #Sacar Fuera
            for line in response:
                data = re.findall("([\w\W]+): ([\w\W]+)", line)
                if data != list(): self.headers[data[0][0]]=data[0][1]
            print(self.headers)
            key = self.headers["Sec-WebSocket-Key"]
            key += "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
            key = hashlib.sha1(bytes(key, "utf-8"))
            key = base64.b64encode(key.digest()).decode("utf-8")
            accept = "HTTP/1.1 101 Switching Protocols\r\n"
            accept += "Upgrade: websocket\r\n"
            accept += "Connection: Upgrade\r\n"
            accept += "Sec-WebSocket-Accept: {}\r\n\r\n".format(key)
            self.send(accept)
            print("Conectado a {}".format(self.addr)) ##Hacer un objeto nuevo

    def list_modules(self):
        modules = dict()
        for path_name in self.app.config.path.modular:
            ruta = self.app.config.path.modular[path_name]
            files = list()
            if ruta:
                for f in os.listdir(ruta):
                    if f and os.path.isfile(os.path.join(ruta, f)):
                        files.append(f)     
                    
                for file_ in files:
                    base, extension = os.path.splitext(file_)
                    if extension in (".py", ".pyw"):
                        path = os.path.join(ruta, file_)
                        name = modular.ModuleLoader(path).name
                        modules[name] = dict()
                        modules[name]["path"] = path
                        modules[name]["scripts"] = list()
                        for mod in modular.ModuleLoader(os.path.join(ruta, file_)).scripts:
                            modules[name]["scripts"].append(mod)
        return modules

    def send(self, data, mask=False): #Make it corroutine
        import random
        try:
            print(data)
            output = io.BytesIO()
            # Prepare the header
            head1 = 0b10000000
            head1 |= 0x01
            head2 = 0b10000000 if mask else 0
            length = len(data)
            if length < 0x7e:
                output.write(struct.pack('!BB', head1, head2 | length))
            elif length < 0x10000:
                output.write(struct.pack('!BBH', head1, head2 | 126, length))
            else:
                output.write(struct.pack('!BBQ', head1, head2 | 127, length))
            if mask:
                mask_bits = struct.pack('!I', random.getrandbits(32))
                output.write(mask_bits)

            # Prepare the data
            if mask:
                data = bytes(b ^ mask_bits[i % 4] for i, b in enumerate(data))
            output.write(bytes(data, "utf-8"))
            self.conn.sendall(output.getvalue())
        except:
            pass

    def start_script(self, module, script, *args, **kwargs):
        try:
            for message in self.get_script(module, script).execute(*args, **kwargs):
                if isinstance(message, dict):
                    if "send" in message:
                        if isinstance(message["send"], dict):
                            for to_send in message["send"]:
                                data = [to_send]+[message["send"][to_send]]
                                self.send("{}\r\n".format(json.dumps(data, separators=(',', ':'))))
                        elif isinstance(message["send"], str):
                            self.send(message["send"]+"\r\n")
                    if "finish" in message:
                        return message["finish"]
        except Exception as e:
            raise #handle it, please
            #TODO

    def get_script(self, module, script):
        modules = self.list_modules()
        if module in modules:
            if script in modules[module]["scripts"]:
                return modular.ModuleLoader(modules[module]["path"]).get_script(script)
            else:
                print("Script: {}".format(script))
                raise AttributeError
        else:
            print("Module: {} not in {}".format(module, modules))
            raise AttributeError
        
