import comtypes, comtypes.client
import os

ALTITUDE_PATH = r"C:\Program Files (x86)\Altitude\Altitude uCI 8\Altitude uAgent Windows"
comtypes.client.GetModule(os.path.join(ALTITUDE_PATH, "Altitude.uAgentWin.API.tlb"))
comtypes.client.GetModule(os.path.join(ALTITUDE_PATH, "Altitude.uAgentWin.Application.API.tlb"))
comtypes.client.GetModule(os.path.join(ALTITUDE_PATH, "Altitude.uAgentWin.Engine.Control.tlb"))
from comtypes.gen.Altitude_uAgentWin_Engine_Control import uAgentEngineControl8 as api
from comtypes.gen.Altitude_uAgentWin_Application_API import uAgentWindowsApplicationAPI8 as appapi
from comtypes.gen.Altitude_uAgentWin_API import uAgentAPIEvents
import configparser
import re
import uuid
import shutil
import time

'''
Altitude 8 uAgent Pythonised Wrapper for Transcom.
'''


class Path(object):
    def __init__(self, config, base=os.environ["HOMEPATH"], extra=None):
        adicional = str()
        base = os.path.abspath(base)
        if base == os.path.abspath(os.environ["HOMEPATH"]):
            adicional = r"\AppData\Local"
        zashel = r"{}{}\Zashel".format(base, adicional)
        if not os.path.exists(zashel):
            os.mkdir(zashel)
        if not extra:
            finalpath = r"{}{}\Zashel\uAgentPy".format(base, adicional)
        else:
            finalpath = r"{}{}\Zashel\uAgentPy\{}".format(base, adicional, extra)
            if not os.path.exists(r"{}{}\Zashel\uAgentPy".format(base, adicional)):
                os.mkdir(r"{}{}\Zashel\uAgentPy".format(base, adicional))
        if not os.path.exists(finalpath):
            os.mkdir(finalpath)
        self.homepath = os.path.abspath(os.environ["HOMEPATH"])
        self.base = finalpath
        self._config = config

    @property
    def config(self):
        return r"{}\config.ini".format(self.base)

    @property
    def download(self):
        return os.path.join(self.homepath, "downloads")

    @property
    def temp(self):
        temp = r"{}\temp".format(self.base)
        if not os.path.exists(temp):
            os.mkdir(temp)

        class Temp():
            def __init__(self, path):
                folder = uuid.uuid1().hex
                final_path = os.path.join(path, folder)
                os.mkdir(final_path)
                self._path = final_path

            def __del__(self):
                shutil.rmtree(self.path, True)

            @property
            def path(self):
                return self._path

        return Temp(temp)

    @property
    def base_temp(self):
        return r"{}\temp".format(self.base)

        ####################    ###   ###   ###
        #                   #  #   # #   # #   #
        # Principal Wrapper #  ##### ####  ####
        #                   #  #   # #     #
        ####################  #   # #     #


API = comtypes.client.CreateObject(api)
AppAPI = None
# Entorno
MAX_API_ROWS = API.Constants.MaxCursorFetchRows
MAX_ROWS = MAX_API_ROWS


class App(object):
    '''
    Principal Class of uAgentAPI.
    '''
    API = API

    #############################
    #                            #
    # Pre-enum class:            #
    #             QueryMatchType #
    #                            #
    #############################

    class QueryMatchType:
        NoValue = -1
        ExactMatch = 0
        AllWords = 1
        AnyWord = 2

        #############################
        #                            #
        # App: __init__              #
        #      __del__               #
        #                            #
        #############################

    def __init__(self, path=None, *, pathclass=Path):
        self.config = Config(path, pathclass)  # We create a 'Config' object to access the configuration
        # self.parsers = list()

    def __del__(self):
        self.logout()
        # self.parsers = None
        if AppAPI and AppAPI.CanExit():
            try:
                AppAPI.Exit()
            except:
                pass

    @property
    def is_logged(self):
        try:
            return API.GetAgentLoginName()
        except:
            return False

            #############################
            #                            #
            # App: Methods            1  #
            #                            #
            #############################

    def call_direct(self, number):
        API.GlobalPhoneDial("={}".format(str(number)), "", "")

    def hang_up(self):
        try:
            API.GlobalPhoneHangUp()
        except:
            pass

    def campaign_open(self, campaign):
        '''
        La dejamos con la telefonía abierta porque así mola más.
        '''
        try:
            API.CampaignOpen(campaign)
        except:
            raise
        try:
            API.CampaignSignOn(campaign)
        except:
            raise

    def campaign_set_not_ready(self, campaign, reason):
        '''
        Ponemos el AUX en una campaña. CACA.
        '''
        try:
            API.CampaignSetNotReady(campaign, API.GetNotReadyReasons().Index(reason))
        except:
            raise

    def login(self, *, instance=None, username=None, password=None, secureconnection=None,
              setcontext=True, site=None, team=None, extension=None):
        global API, AppAPI
        '''
        First we validate if uAgent is Logged and, if it is true, attach to it.
        Elsehow login to the server. If setcontext is True, it will setted as given
        "site", "team" and "extension".
        '''
        if not username:
            if "username" in self.config.server:
                username = self.config.server["username"]
            else:
                raise Exception("Falta nombre de usuario")
        if not password:
            if "password" in self.config.server:
                password = self.config.server["password"]
            else:
                password = None
        AppAPI = comtypes.client.CreateObject(appapi)
        if AppAPI.CanAttach():
            API = AppAPI.Attach(username, password)
            App.API = API
        else:
            AppAPI.Exit()
            AppAPI = None
            # API = comtypes.client.CreateObject(api)
            # Verificamos los datos para control de excepciones
            if not instance:
                if "instance" in self.config.server:
                    instance = self.config.server["instance"]
                else:
                    raise Exception("Falta nombre de la instancia")
            if secureconnection == None:
                if "secureconnection" in self.config.server:
                    secureconnection = self.config.server["secureconnection"] == "True"
                else:
                    secureconnection = False
            try:
                API.Login(instance, username, password, secureconnection)
            except:
                raise

            # Si está definido el contexto, lo implicamos directamente.
            # ¿Para qué esperar?
            if setcontext:
                try:
                    self.set_login_context(site=site, team=team, extension=extension)
                except:
                    raise
        self.set_event_handler(DefaultEventHandler())

    def logout(self):
        # for sql in self.parsers:
        # del(sql)
        if AppAPI and AppAPI.CanDetach():
            AppAPI.Detach()
        else:
            API.CleanUpAgent(True)

    def execute(self, sql, bind_list=tuple()):
        sql = SqlParser(sql, bind_list)
        # self.parsers.append(sql)
        return sql

        #############################
        #                            #
        # App: Methods            2  #
        #                            #
        #############################

    def search_contacts(self, campaign, sqlwhere):  # Bullshit
        '''
        Returns a list with 50 contacts per item
        '''
        inicial = 0
        maxim = MAX_ROWS
        finalcontacts = list()
        contacts = True
        while contacts:
            cursor = API.CreateContactsCursor(campaign, 2, sqlwhere)
            for counter in range(5):
                contacts = API.FetchContactsCursor(campaign, cursor, inicial, maxim)
                finalcontacts.append(contacts)
                inicial += maxim
            API.CloseContactsCursor(campaign, cursor)
            yield finalcontacts

    def set_event_handler(self, handler):
        self._event_handler = comtypes.client.GetEvents(API, handler, uAgentAPIEvents)

    def set_login_context(self, site=None, team=None, extension=None):
        if not site:
            if "site" in self.config.server:
                site = self.config.server["site"]
            else:
                site = "Madrid"
        if not team:
            if "team" in self.config.server:
                team = self.config.server["team"]
            else:
                team = ""
        if not extension:
            if "extension" in self.config.server:
                extension = self.config.server["extension"]
            else:
                raise Exception("Falta extensión de logado")
        try:
            API.SetLoginContext(site, team, extension)
        except:
            raise

    def set_not_ready(self, reason):
        '''
        Ponemos el AUX en todas las campañas. MOLA
        '''
        try:
            API.GlobalSetNotReady(API.GetNotReadyReasons().Index(reason))
        except:
            raise

            ################    ####  ###  #      ###   ###   ###   ####  ####  ###
            #               #  #     #   # #     #   # #   # #   # #     #     #   #
            # Parser of SQL #  ##### #   # #     ####  ##### ####  ##### ###   ####
            #               #      # #  ## #     #     #   # #  #      # #     #  #
            ################  ####   ####  #### #     #   # #   # ####   #### #   #


class SqlParser(object):
    '''
    Parser of the SQL
    '''

    #############################
    #                            #
    # SqlParser: __init__        #
    #            __del__         #
    #                            #
    #############################

    def __init__(self, sql, bind_list=tuple()):
        self._freezed = False
        self._count = int()
        self._tables = list()
        self._columns = list()
        self._where = str()
        self._items = dict()
        self._index = dict()
        self._bind_list = bind_list
        self._iter_index = int()
        if not " delete " in sql and not " drop " in sql:
            self.cursorSQL = -1
            self.lastquery = ""
            self._sql = SqlParser.parse_sql(sql, self._bind_list)
            if self._sql.strip()[0:7].lower() == "select ":
                self._count = self.get_count()
            print(self._count)
            self.execute(self.sql)

        else:
            raise Exception("No seas malo")

    def __del__(self):
        if not self.freezed:
            self.close_cursor()

            #############################
            #                            #
            # SqlParser: __getitem__     #
            #            __iter__        #
            #            __getattr__     #
            #                            #
            #############################

    def __getitem__(self, key):
        if key < self.count:
            try:
                page = int(key / MAX_ROWS) + 1
                subkey = key - ((page - 1) * MAX_ROWS)
                if page not in self.items:
                    data = self.fetch_page(page)
                    # item = Item(data, self.columns, subkey)
                    return self.items[page].set_row(key)
                else:
                    return self.items[page].set_row(key)
            except:
                raise IndexError
        else:
            raise IndexError("EOL")

    def __iter__(self):
        return self

    def __next__(self):
        now = self._iter_index
        self._iter_index += 1
        value = None
        try:
            # print(now)
            value = self[now]
        except:
            self._iter_index = int()
            raise StopIteration
        return value

    def __getattr__(self, attribute):
        if attribute in self.index:
            class Index:
                def __init__(self, parser, index):
                    self.parser = parser
                    self.index = index

                def __call__(self, value, method=App.QueryMatchType.NoValue):
                    return self.parser.get_index(self.index, value, method)

            return Index(self, attribute)

        else:
            super().__getattr__(self, attribute)


            #############################
            #                            #
            # SqlParser: Properties      #
            #                            #
            #############################

    @property
    def columns(self):
        return self._columns

    @property
    def count(self):
        return int(self._count)

    @property
    def freezed(self):
        return self._freezed

    @property
    def index(self):
        return self._index

    @property
    def items(self):
        return self._items

    @property
    def pages(self):
        real = self.count / MAX_ROWS
        part = real - int(real)
        return int(real) + (part > 0 and 1 or 0)

    @property
    def sql(self):
        return self._sql

    '''@sql.setter
    def sql(self, value):
        if not self.freezed:
            self._sql = value
            self._count = self.get_count()
            self._items = dict()
            try:
                self.cursorSQL = API.OpenSqlCursor(self.cursorSQL, self.sql, self.lastquery
            except:
                print("Cursor: {}\nSql: {}\nLastQuery: {}\n".format(self.cursorSQL, self.sql, self.lastquery))
                raise
        else:
            raise AttributeError("can't set attribute'")'''

    @property
    def tables(self):
        return self._tables

    @property
    def where(self):
        return self._where


        #############################
        #                            #
        # SqlParser: Static Methods  #
        #                            #
        #############################

    @staticmethod
    def get_tables(sql):
        tablas = re.findall(r"(from |[\w]+ join )([\w_\.]+)( as [\w_\.]+)?", sql.lower())
        # print(tablas)
        tablas_final = dict()
        for tabla in tablas:
            if tabla[2] != str():
                tablas_final[tabla[1]] = tabla[2].replace(" as ", "")
            else:
                tablas_final[tabla[1]] = tabla[1]
        # print(tablas_final)
        return tablas_final

    @staticmethod
    def get_where(sql):
        if " where " in sql:
            if " group by " in sql:
                return re.findall(r"(?<=where )([\w+ \'=<>()\.\-#:%]+)(group by [\w+ ,]+)?", sql.lower())[0]
            else:
                return re.findall(r"(?<=where )([\w+ \'=<>()\.\-#:%]+)(order by [\w+ ,]+)?", sql.lower())[0]
        else:
            return ["", ""]

    @staticmethod
    def parse_sql(sql, bind_list=tuple()):
        sql = sql.replace("\n", " ")
        sql = sql.replace(";", "")
        sql_pieces = sql.split("?")
        sql_final = str()
        if len(bind_list) > 0:
            for index, piece in enumerate(sql_pieces):
                if index < len(sql_pieces) - 1:
                    if "like" in piece:
                        sql_final += "{}'%{}%'".format(piece,
                                                       str(bind_list[index]).replace("'", "''").replace("\"", "\"\""))
                    else:
                        sql_final += "{}'{}'".format(piece,
                                                     str(bind_list[index]).replace("'", "''").replace("\"", "\"\""))
                else:
                    sql_final += piece
        if sql_final == str():
            sql_final = sql
        # print(sql_final)
        return sql_final

        #############################
        #                            #
        # SqlParser: Class Methods 1 #
        #                            #
        #############################

    def close_cursor(self):
        if not self.freezed and self.cursorSQL != -1:
            API.CloseSQLCursor(self.cursorSQL)
            # print("Closed {} cursor".format(str(self.cursorSQL)))
        elif self.freezed:
            raise Exception("No se puede cerrar un cursor ya cerrado.")

    def execute(self, sql, bind_list=()):
        if not self.freezed:
            for x in range(2):
                try:
                    self.cursorSQL = API.OpenSqlCursor(self.cursorSQL, sql, self.lastquery)
                    break
                except:
                    # print("Cursor: {}\nSql: {}\nLastQuery: {}".format(self.cursorSQL, sql, self.lastquery))
                    if x == 4: raise
                    time.sleep(1)
            self.lastquery = sql

        else:
            raise Exception("No se puede ejecutar nada con el cursor cerrado.")

    def fetch_page(self, page, save=True):
        '''
        Returns the indicated page. Starts by 1.
        '''
        if not self.freezed and not page in self.items:
            inicial = (page - 1) * MAX_ROWS
            data = API.FetchSqlCursor(self.cursorSQL, inicial, MAX_ROWS)
            # print("Fetched {} rows in page {}".format(str(data.rowcount), str(page)))
            if save:
                item = Item(data, self.columns, (page - 1) * MAX_ROWS, self.count)
                self._items[page] = item
            return data
        elif page in self.items:
            return self.items[page].fetched
        else:
            raise Exception("No se puede hacer peticiones al servidor con el cursor cerrado.")

    def fetch_part(self, save=True):
        '''
        Returns a generator!
        '''
        if not self.freezed:
            page = 1
            total = 1
            inicial = 0
            while total > 0:
                fetched = API.FetchSqlCursor(self.cursorSQL, inicial, MAX_ROWS)
                # print("Fetched {} rows in page {}".format(str(fetched.rowcount), str(page)))
                total = fetched.RowCount
                inicial += MAX_ROWS
                if save:
                    item = Item(fetched, self.columns, (page - 1) * MAX_ROWS, self.count)
                    self._items[page] = item
                page += 1
                yield fetched
        else:
            raise Exception("No se puede hacer peticiones al servidor con el cursor cerrado.")

    def freeze(self, asis=False):
        '''
        Freeze the query
        If asis is True get all the results before freezing
        '''
        if not asis:
            import datetime
            ahora = datetime.datetime.now()
            for x in range(self.pages):
                self.fetch_page(x + 1)
            total = datetime.datetime.now() - ahora
            print("Terminado en {} segundos".format(str(total.total_seconds()).replace(".", ",")))
        self._freezed = True

        #############################
        #                            #
        # SqlParser: Class Methods 2 #
        #                            #
        #############################

    def get_columns(self, sql):
        if not self.freezed:
            sql = sql.replace("\n", " ")
            if not " from" in sql.lower():
                columns = re.findall(r"(?<=select )([\w+ ,()\*\@\[\]\.\-_'<>=/\+]+)", sql.lower())
                print("not from")
            else:
                if not "select distinct " in sql.lower()[:32]:
                    columns = re.findall(r"(?<=select )([\w+ ,()\*\@\[\]\.\-_'<>=/\+]+) from", sql.lower())
                else:
                    columns = re.findall(r"(?<=select distinct )([\w+ ,()\*\@\[\]\.\-_'<>=/\+]+) from", sql.lower())
                print(columns)
            try:
                columns = columns[0].split(",")
                columns = [columns[x].strip() for x in range(len(columns))]
                indice = None
                for index, column in enumerate(columns):
                    if " from " in column:
                        columns[index] = columns[index].split(" ")[0]
                        if not indice: indice = index + 1
                columns = columns[:indice]
                be_columns = list()
                last_column = str()
                counter = int()
                for column in columns:
                    if "(" in column and ")" not in column:
                        counter += 1
                        last_column += column
                    elif ")" in column and "(" not in column and counter == 1:
                        be_columns.append("{}, {}".format(last_column, column))
                        last_column = str()
                        counter -= 1
                    elif last_column != str():
                        last_column += ", {}".format(column)
                    else:
                        be_columns.append(column)
                for index, column in enumerate(be_columns):
                    if "=" in column and "(" in column and (
                                    column.index("(") > column.index("=") or
                                (")" in column and column.index(")") < column.index("="))):
                        be_columns[index] = re.findall(r"([\w ]+)=", column)[0].strip()
                columns = be_columns

            except:
                print(sql)
                print(columns)
                raise
            final_columns = list()
            table = str()
            for column in columns:
                if "*" in column:
                    table = re.findall(r"([\w]+)\.\*", column.lower())  # Revisar esto
                    table_columns = self.get_columns_names(table)
                    for table in table_columns:
                        for col in table_columns[table]:
                            final_columns.append("{}.{}".format(self.tables[table], col))
                            '''
                            try:
                                if col in final_columns: final_columns.append("{}.{}".format(table[0], col))
                                else: final_columns.append(col)
                            except:
                                raise
                        '''
                else:
                    if " as " in column:
                        column = re.findall(r"(?<= as )([\w\._\-]+)", column)[-1]
                    final_columns.append(column)

            self._sql = self._sql.replace("*", ", ".join(final_columns))
            if final_columns != list():
                # self._sql = self._sql.replace("*", ", {}.".format(table).join(final_columns))
                self._sql = self._sql.replace("*.{}".format(table), ", ".join(final_columns))
            return final_columns
        else:
            return self.columns

    def get_columns_names(self, table_name=list()):
        if table_name == list():
            table_name = [table for table in self.tables]
        final = dict()
        if not self.freezed:
            for table in table_name:
                # print("Table: {}".format(table))
                if not "information_schema" in table:
                    db = str()
                    tabled = table
                    if "dbo" in table:
                        db, tabled = re.findall("([\w]+.)dbo.([\w]+)", table)[0]
                    sql = "select column_name from {}information_schema.columns where table_name='{}'".format(db,
                                                                                                              tabled)
                    # print("Sql: {}".format(sql))
                    sqlnames = SqlParser(sql)
                    names = list()
                    for x in range(sqlnames.pages):
                        pagina = sqlnames.fetch_page(x + 1, False)
                        for y in range(MAX_ROWS):
                            try:
                                names.append(pagina.Index(y, 0))
                            except:
                                break
                    if table not in final:
                        final[table] = list()
                    final[table].extend(names)
            # print("Total Columnas: {}".format(len(final)))
            return final
        else:
            return self.columns

    def get_count(self):
        '''
        Cuenta los registros. Como es necesario pasar antes por get_tables y get_columns y get_where...
        TODO: Implementar "JOIN"
        '''
        if (not self.freezed and self.sql != str() and
                    "insert into " not in self.sql.lower() and
                    "update " not in self.sql.lower()):
            self._tables = SqlParser.get_tables(self.sql)
            print("Aquí: " + self.sql)
            self._columns = self.get_columns(self.sql)
            self._where = SqlParser.get_where(self.sql)
            sql = self.sql
            if " order by " in sql:
                sql = re.findall("([\w\W]+) order", sql)[0]
            sql = "select count(*) from ({})a".format(sql)
            try:
                self.execute(sql)
            except:
                raise
            # print(sql)
            data = self.fetch_page(1, False)
            final_count = data.Index(0, 0)
            # print("Total: {}".format(str(final_count)))
            return final_count
        else:
            return self.count


            #############################
            #                            #
            # SqlParser: Class Methods 3 #
            #                            #
            #############################

    def get_index(self, index, value, method=App.QueryMatchType.NoValue):
        class GetterIndex:
            def __init__(self, parser, index):
                self.parser = parser
                self.index = list(index)

            def __getitem__(self, pos):
                return self.parser[int(self.index[pos])]

            @property
            def count(self):
                return len(self.index)

        if index in self.index:
            final = None
            if value in self.index[index]:
                final = GetterIndex(self, self.index[index][value])
            return final
        else:
            raise Exception("index not defined")

    def set_index(self, index):
        if self.freezed:
            if index in self.columns and not index in self.index:
                self.index[index] = dict()
                for ind, item in enumerate(self):
                    key = item.get_column(index)
                    if key not in self.index[index]:
                        self.index[index][key] = set()
                    self.index[index][key].add(ind)

    def primary_key(self, table):
        sql = "select i.name as index_name, COL_NAME(ic.object_id, ic.column_id) as column_name, ic.index_column_id, ic.key_ordinal, ic.is_included_column from sys.indexes as i inner join sys.index_columns as ic on i.object_id = ic.object_id and i.index_id = ic.index_id where i.object_id = OBJECT_ID('{}') and i.is_primary_key=1".format(
            table)
        key = SqlParser(sql)
        return key[0].column_name

    def unique_key(self, table):
        sql = "select i.name as index_name, COL_NAME(ic.object_id, ic.column_id) as column_name, ic.index_column_id, ic.key_ordinal, ic.is_included_column from sys.indexes as i inner join sys.index_columns as ic on i.object_id = ic.object_id and i.index_id = ic.index_id where i.object_id = OBJECT_ID('{}') and i.is_unique=1".format(
            table)
        key = SqlParser(sql)
        return key[0].column_name


        #######################    ###   ###  #   #  #### #  ###
        #                      #  #     #   # ##  # #     # #
        # Configuration Driver #  #     #   # # # # ###   # # ###
        #                      #  #     #   # #  ## #     # #   #
        #######################   ###   ###  #   # #     #  ###


class Config(configparser.ConfigParser):
    '''
    Configuration Class.
    server
      instance
      username
      password
      secureconnection
      site
      team
      extension
    '''

    #############################
    #                            #
    # Config: __init__           #
    #                            #
    #############################

    def __init__(self, path=None, pathclass=Path, **kwargs):
        '''
        Initializacion of the object Config.
        config_file = path of the configuration file. config.ini as default.
        kwargs as kwargs of initialized method
        '''
        # if config_file==None:
        #    config_file = "config.ini"
        # Si config_file es None, no se guarda. Sea.
        if path:
            self.path = pathclass(self, path)
        else:
            self.path = pathclass(self)
        configparser.ConfigParser.__init__(self, allow_no_value=True)
        if self.path.config and os.path.exists(self.path.config):
            self.read(self.path.config)
        else:
            self.initialize(**kwargs)

            #############################
            #                            #
            # Config: Properties         #
            #                            #
            #############################

    @property
    def server(self):
        return self["server"]

        #############################
        #                            #
        # Config: Static Methods     #
        #                            #
        #############################

        #############################
        #                            #
        # Config: Methods            #
        #                            #
        #############################

    def initialize(self, *, instance="sesmapaltas01:1500",
                   username=None, password=None, secureconnection=False,
                   site="Madrid", team=None, extension=None,
                   **kwargs):
        '''
        Initialize the config file with the most basic configurations
        '''
        self["server"] = {
            "instance": instance,
            "username": username,
            "password": password,
            "secureconnection": secureconnection,
            "site": site,
            "team": team,
            "extension": extension
        }
        self.save()

    def reload_ini(self):
        self.read(self.path.config)

    def save(self):
        '''
        Save da file you madafacaaaaa
        '''
        if self.path.config:
            with open(self.path.config, "w") as config_file:
                self.write(config_file)

                ################   # #####  ### #   #
                #               #  #   #   #    ## ##
                # Parsed Atom   #  #   #   ###  # # #
                #               #  #   #   #    #   #
                ################  #   #    ### #   #


class Item():
    def __init__(self, fetched, column_list, row, total):
        self.fetched = fetched
        if column_list != list():
            self.column_list = column_list
        else:
            self.column_list = ["Column{}".format(str(x)) for x in range(self.fetched.columncount)]
        self.item_column_list = list(self.column_list)
        self.column_list = [col.replace(".", "_") for col in self.column_list]
        self.total = total
        self.row = row
        self._iter_index = 0

    def __getattr__(self, attribute):
        if attribute in self.column_list:
            return self.get_row(self.row, attribute)
        else:
            for item in self.column_list:
                if attribute.lower() == item[0 - len(attribute):].lower():
                    return self.get_row(self.row, item)
            raise AttributeError

    def __getitem__(self, item):
        if item in self:
            return self.__getattr__(item.replace(".", "_"))
        else:
            raise IndexError

    def __contains__(self, item):
        return item in self.item_column_list

    def __iter__(self):
        for key in self.item_column_list:
            yield key, self[key]

            # def __next__(self):
            # try:
            #    row = self.get_row(self.row, column)
            #    self._iter_index += 1
            #    return row
            # except:
            #    self._iter_index = 0
            #    raise StopIteration

    def __repr__(self):
        final = "{}\n".format("-" * 40)
        row = "{} Row Number {} ".format("-" * 10, str(self.row))
        final += "{}{}\n".format(row, "-" * (40 - (len(row))))
        for column in self.column_list:
            final += "{}: {}\n".format(column, self.__getattr__(column))
        final += "{}{}\n".format(row, "-" * (40 - (len(row))))
        final += "{}\n".format("-" * 40)
        return final
        return "<Row number {}>".format(self.row)

    @property
    def row(self):
        return self._row

    @row.setter
    def row(self, value):
        if self.get_subrow(value) >= self.total:
            raise IndexError
        else:
            self._row = value

    def get_column(self, column):
        return self.__getattr__(column)

    def get_subrow(self, row):
        return row - MAX_ROWS * int(row / MAX_ROWS)

    def get_row(self, row, column):
        try:
            subrow = self.get_subrow(row)
            if column in self.column_list and subrow < self.fetched.rowcount:
                fetched = self.fetched.Index(subrow, self.column_list.index(column))
                return fetched
            else:
                raise Exception("Algo no cuadra en lo dado en Item()")
        except:
            raise IndexError

    def set_row(self, row):
        self.row = row
        return self

        ##################
        #                 #
        # Events Handler  #
        #                 #
        ##################


class DefaultEventHandler(object):
    '''
    Default Event Handler. Nothing to do with it.
    '''

    def ActiveSessionChanged(self, this, activeSessionId):
        pass

    def AgentAlteredMessageOfTheDay(self, this, newMessage):
        pass

    def CampaignAvailable(self, this, campaign):
        pass

    def CampaignFeatureErrorEvent(self, this, campaign,
                                  requestType, progressType):
        pass

    def CampaignFeatureEvent(self, this, campaign, featureInvokedType):
        pass

    def CampaignSuspended(self, this, campaign):
        pass

    def CampaignUnavailable(self, this, campaign):
        pass

    def ConnectionStateChanged(self, this, connectionState):
        pass

    def ExtensionChanged(self, this, newExtension):
        pass

    def ExtensionCleared(self, this):
        pass

    def InstanceAlteredMessageOfTheDay(self, this, newMessage):
        pass

    def SessionContactLoadedEvent(self, this, sessionID, data):
        pass

    def SessionContactProfileUpdatedEvent(self, this, sessionID, data):
        pass

    def SessionContactScheduleUpdatedEvent(self, this, sessionID, newSchedule):
        pass

    def SessionDataEvent(self, this, sessionID, data, reason):
        pass

    def SessionDataTransactionEnded(self, this, sessionID, endReason):
        pass

    def SessionDataTransactionEnding(self, this, sessionID, endReason):
        pass

    def SessionEmailErrorEvent(self, this, sessionID, data):
        pass

    def SessionEmailEvent(self, this, sessionID, data):
        pass

    def SessionEmailProgressEvent(self, this, sessionID, data):
        pass

    def SessionEmailSentEvent(self, this, sessionID, data):
        pass

    def SessionEnded(self, this, sessionID):
        pass

    def SessionIMErrorEvent(self, this, sessionID, data):
        pass

    def SessionIMEvent(self, this, sessionID, data):
        pass

    def SessionIMMessageEvent(self, this, sessionID, data):
        pass

    def SessionIMProgressEvent(self, this, sessionID, data):
        pass

    def SessionPhoneErrorEvent(self, this, sessionID, data):
        pass

    def SessionPhoneEvent(self, this, sessionID, data):
        pass

    def SessionPhoneProgressEvent(self, this, sessionID, data):
        pass

    def SessionStarted(self, this, sessiondata):
        pass

    def SessionWorkflowErrorEvent(self, this, sessionID, data):
        pass

    def SessionWorkflowEvent(self, this, sessionID, data):
        pass

    def SessionWorkflowProgressEvent(self, this, sessionID, data):
        pass

    def TeamAlteredMessageOfTheDay(self, this, newMessage):
        pass

    def UserMessage(self, this, message):
        pass

        ####################################################
        #                                                   #
        # TODO: Other classes not implemented yet           #
        #                                                   #
        ####################################################


class Session():
    '''
    Session Class indicating a session in uAgent
    TODO
    '''
    pass


class Campaign():
    '''
    Campaign Class
    '''


