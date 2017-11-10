'''
Altitude 8 uAgent Pythonised Wrapper.

Made by Iván Uría & Roberto Cabezuelo
'''

import comtypes, comtypes.client
import datetime
import queue
import socket
import subprocess
import sys
import os

ALTITUDE_PATH = r"C:\Program Files (x86)\Altitude\Altitude uCI 8\Altitude uAgent Windows"
comtypes.client.GetModule(os.path.join(ALTITUDE_PATH, "Altitude.uAgentWin.API.tlb"))
comtypes.client.GetModule(os.path.join(ALTITUDE_PATH, "Altitude.uAgentWin.Application.API.tlb"))
comtypes.client.GetModule(os.path.join(ALTITUDE_PATH, "Altitude.uAgentWin.Engine.Control.tlb"))
from comtypes.gen.Altitude_uAgentWin_Engine_Control import uAgentEngineControl8 as api
from comtypes.gen.Altitude_uAgentWin_Application_API import uAgentWindowsApplicationAPI8 as appapi
from threading import Thread
from zashel.utils import daemonize
from comtypes.gen.Altitude_uAgentWin_API import uAgentAPIEvents, UAAttributeList, UAAttribute, UAByteList, UAExtendPurpose_Unknown
from multiprocessing import Pipe
from multiprocessing.managers import BaseManager
import configparser
import getpass
import re
import uuid
import shutil
import time


HOST=socket.gethostbyname(socket.gethostname())
PORT = 50005


def strptime(data):
    """
    Converts date to datetime.datetime object. It parses 01/01/1900 as void string by default.
    :param data: String containing date to parse.
    :return: datetime.datetime object if data is a %Y-%m-%d string, str() if it is 01/01/1900 or data whatelse.
    """
    if data == "":
        return data
    elif data == "1900-01-01 00:00:00":
        return ""
    else:
        try:
            return datetime.datetime.strptime(data, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                return datetime.datetime.strptime(data, "%Y-%m-%d")
            except ValueError:
                return data


def to_float(data):
    """
    Converts data to float.
    :param data: String to convert to float.
    :return: float object or data if it is str().
    """
    if data == "":
        return data
    else:
        return float(data.replace(",", "."))


def decimal(data):
    """
    Converts data to float with two decimals.
    :param data: String to convert to float.
    :return: float object or data if it is str().
    """
    if data == "":
        return data
    else:
        return round(float(data.replace(",", ".")), 2)


def to_int(data):
    """
    Converts data to int.
    :param data: String to convert to int.
    :return: int object or data if it is str().
    """
    if data == "":
        return data
    else:
        return int(data)


class CampaignNotReadyError(Exception):
    """
    Exception to raise in case the Campaign is not ready.
    """
    pass


class Path(object):
    """
    Path object to represent common used paths in uAgentAPI
    """
    def __init__(self, config, base=os.environ["HOMEPATH"], extra=None):
        """
        Initializes Path.
        :param config: Config instance.
        :param base: Base path to make app-user paths
        :param extra: Aditional folder to append to final path.
        """
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
        """
        Configuration file path.
        """
        return r"{}\config.ini".format(self.base)

    @property
    def download(self):
        """
        Download folder path
        """
        return os.path.join(self.homepath, "downloads")

    @property
    def temp(self):
        """
        New temporary folder path.
        """
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
        """
        Base path tocrete temporary folders.
        """
        return r"{}\temp".format(self.base)

        ####################    ###   ###   ###
        #                   #  #   # #   # #   #
        # Principal Wrapper #  ##### ####  ####
        #                   #  #   # #     #
        ####################  #   # #     #


API = None
AppAPI = None
# Entorno
#MAX_API_ROWS = API.Constants.MaxCursorFetchRows
MAX_API_ROWS = 50
MAX_ROWS = MAX_API_ROWS
DATA_TYPES = {"datetime": strptime,
              "decimal": decimal,
              "int": to_int,
              "smallint": to_int,
              "tinyint": to_int,
              "varchar": str,
              "float": to_float,
              "real": to_float}

#############################
#                            #
# Pre-enum class:            #
#             QueryMatchType #
#                            #
#############################

class QueryMatchType:
    """
    Enumerate to set how the wuery may execute.
    """
    NoValue = -1
    ExactMatch = 0
    AllWords = 1
    AnyWord = 2

class _App:
    '''
    Principal Class of uAgentAPI. It is managed by a multiprocess.managers.Manager instance.
    '''
    API = API
    AppAPI = AppAPI
    INSTANCE = None

        #############################
        #                            #
        # App: __init__              #
        #      __del__               #
        #                            #
        #############################

    def __init__(self, path=None, *, pathclass=Path):
        """
        Initilizes _App
        :param path: Base path to configuration file. None by default.
        :param pathclass: Base class to app-user files. Path by default.
        """
        self.config = Config(path, pathclass)
        _App.API = comtypes.client.CreateObject(api)
        _App.INSTANCE = self
        self.historic_events = list()
        self.historic_phones = list()
        self.last_phone = None
        self.session_id = None

    def __del__(self):
        """
        Deletes _App instance cleanly.
        """
        self.logout()
        # self.parsers = None
        if _App.AppAPI and _App.AppAPI.CanExit():
            try:
                _App.AppAPI.Exit()
            except:
                pass

    @property
    def campaigns(self):
        """
        Avalilable campaigns to loggedin user.
        :return: list of available campaigns.
        """
        try:
            campaigns = _App.API.GetCampaigns()
            return [campaigns.Index(index).name for index in range(campaigns.Count)]
        except (comtypes.COMError, AttributeError):
            pass

    @property
    def is_logged(self):
        """
        Returns loggedin username of false whatelse.
        :return:
        """
        try:
            return _App.API.GetAgentLoginName()
        except:
            return False
        
    ##### GET Properties #####

    def get_campaigns_names(self):
        """
        Getter to available camapaign names.
        :return: self.campaigns property.
        """
        return self.campaigns

    def get_campaigns(self):
        """
        Getter to available campaings properties.
        :return: list of available campaings properties.
        """
        campaigns = _App.API.GetCampaigns()
        return [{campaigns.Index(index).name: {"ContactsEnded": campaigns.Index(index).ContactsEnded,
                                               "HasOperationPending": campaigns.Index(index).HasOperationPending,
                                               "Id": campaigns.Index(index).Id,
                                               "IsOpen": campaigns.Index(index).IsOpen,
                                               "IsReady": campaigns.Index(index).IsReady,
                                               "IsSignedOn": campaigns.Index(index).IsSignedOn,
                                               "IsSuspended": campaigns.Index(index).IsSuspended,
                                               "MessageOfTheDay": campaigns.Index(index).MessageOfTheDay,
                                               "NotReadyReason": campaigns.Index(index).NotReadyReason,
                                               "OutboundAddresses": campaigns.Index(index).OutboundAddresses}
                 } for index in range(campaigns.Count)]

    def get_campaign_statistics(self, campaign):
        """
        Getter to specified campaign statistics.
        :param campaign: Campaign name to serch the statistics for.
        :return: Dictionary with statistics of specified campaign.
        """
        data = _App.API.GetCampaignStatistics(campaign)
        return {"AvgDataTransactionDuration": data.AvgDataTransactionDuration,
                "NumAgentsAvailable": data.NumAgentsAvailable ,
                "NumAgentsIdle": data.NumAgentsIdle,
                "NumAgentsInWrapUp": data.NumAgentsInWrapUp,
                "NumAgentsLogged": data.NumAgentsLogged,
                "NumAgentsReady": data.NumAgentsReady,
                "NumInteractionsQueued": data.NumInteractionsQueued}
    
    def get_config(self):
        """
        Getter to self.config property.
        :return: self.config property.
        """
        return self.config
    
    def get_historic_events(self):
        """
        Getter to self.historic_events property.
        :return: self.historic_events property.
        """
        return self.historic_events
    
    def get_historic_phones(self):
        """
        Getter to self.historic_phones proprety.
        :return: self.historic_phones proprety.
        """
        return self.historic_phones

    def get_is_logged(self):
        """
        Getter to self.is_logged property.
        :return: self.is_logged property.
        """
        return self.is_logged
        
    def get_last_phone(self):
        """
        Getter to self.last_phone property.
        :return: self.last_phone property.
        """
        return self.last_phone

            #############################
            #                            #
            # App: Methods            1  #
            #                            #
            #############################

    # Phone Methods
    def answer(self, sessionID):
        """
        Answers an interaction
        :param sessionID: SessionID of opened session
        :return: None
        """
        self.session_id = _App.API.Answer(sessionID)

    def extend(self, extension, campaign=""):
        """
        Extends a phone call to another agent.
        :param extension: Extension to extend the phone call.
        :return: None
        """
        self.session_id = _App.API.PhoneExtend(self.session_id, "", "{}".format(extension), campaign,
                                               "", "", False, False,
                                               comtypes.client.CreateObject(UAAttributeList),
                                               comtypes.client.CreateObject(UAByteList),
                                               UAExtendPurpose_Unknown # 3
                                               )

    def call(self, number):
        """
        Calls directly specified number.exit
        :param number: Number to call to.
        :return: None.
        """
        self.session_id = _App.API.GlobalPhoneDial("{}".format(str(number)), "","")

    def call_direct(self, number):
        """
        Calls directly specified number.
        :param number: Number to call to.
        :return: None.
        """
        self.session_id = _App.API.GlobalPhoneDial("={}".format(str(number)), "", "")

    def discard(self):
        """
        Discards data reservation in database in current session.
        :return: None.
        """
        _App.API.DiscardDataTransaction(self.session_id)

    def end_reservation(self):
        """
        Ends data reservation in database in current session.
        :return: None.
        """
        _App.API.EndReservation(self.session_id)

    def global_hang_up(self):
        """
        Hangs up current call.
        :return: None.
        """
        _App.API.GlobalPhoneHangUp()

    def hang_up(self):
        """
        Hangs up current call.
        :return: None.
        """
        _App.API.PhoneHangUp(self.session_id)

    def hold(self):
        """
        Holds the current call.
        :return: None.
        """
        _App.API.PhoneHold(self.session_id)

    def phone_dial(self, number, media=""):
        """
        Dials specified number.
        :param number: Number to call to.
        :param media: Type of media used to call. str() by default.
        :return: None
        """
        _App.API.SessionPhoneDial(self.session_id, "={}".format(str(number)), "", "", True,
                                  False, False, comtypes.client.CreateObject(UAAttributeList),
                                  comtypes.client.CreateObject(UAByteList), media)

    def retrieve(self):
        """
        Retrieves held call.
        :return: None.
        """
        _App.API.PhoneRetrieve(self.session_id)

    def transfer(self):
        """
        Transfers mutually all open call phones.
        :return: None
        """
        _App.API.PhoneTransfer(self.session_id)

    # Campaign Methods

    def campaign_open(self, campaign, *, open_telephony=True):
        """
        Opens specified campaign.
        :param campaign: Campaign name to open.
        :param open_telephony: Whether or not open telephony in the same step. True by default.
        :return: None
        """
        if not campaign in self.campaigns:
            raise CampaignNotReadyError()
        _App.API.CampaignOpen(campaign)
        if open_telephony is True:
            _App.API.CampaignSignOn(campaign)

    def campaign_open_telephony(self, campaign):
        """
        Opens specified campaign telephony.
        :param campaign: Campaign name to open telephony.
        :return: None.
        """
        _App.API.CampaignSignOn(campaign)

    def campaign_set_ready(self, campaign):
        """
        Sets ready in specified camapaign telephony. It may be opened before.
        :param campaign: Capaign name to set ready to.
        :return: None.
        """
        _App.API.CampaignSetReady(campaign)

    def campaign_set_not_ready(self, campaign, reason):
        """
        Sets aux reason to specified campaign.
        :param campaign: Campaign name to set aux to.
        :param reason: Aux reason number to set to.
        :return: None.
        """
        _App.API.CampaignSetNotReady(campaign, _App.API.GetNotReadyReasons().Index(reason))

    def campaign_change_not_ready_reason(self, campaign, reason):
        """
        Changes aux reason in specified campaign.
        :param campaign: Campaign name to change aux reason to.
        :param reason: New aux reason number to set to.
        :return: None.
        """
        _App.API.CampaignChangeNotReadyReason(campaign, _App.API.GetNotReadyReasons().Index(reason))

    def campaign_start_script(self, campaign):
        """
        Starts a new script in linked uAgent instance and specified campaign.
        :param campaign: Cmapaign name to open script to.
        :return: None
        """
        _App.API.CampaignStartScript(campaign)

    def open_uAgent_and_login(self, *, instance=None, username=None, password=None, secureconnection=False,
                              setcontext=True, site=None, team=None, extension=None):
        """
        Opens uAgent instance, logs in an agent and makes the app visible.
        :param instance: Network path to uAgent Instance.
        :param username: Username to log in uAgent.
        :param password: Password to log in uAgent.
        :param secureconnection: Whether secure connection is used or not. False by default.
        :param setcontext: Whether context is setted or not. If true, site and extension migth be setted.
                           True by default.
        :param site: Site name to connect to.
        :param team: Agent team to connect to.
        :param extension: Extension in site to connect telephony to.
        :return: None
        """
        if _App.AppAPI is not None:
            _App.AppAPI.Exit()
            _App.AppAPI = None
        _App.AppAPI = comtypes.client.CreateObject(appapi)
        try:
            _App.API = _App.AppAPI.Login(instance, username, password, secureconnection)
        except comtypes.COMError:
            if _App.AppAPI and _App.AppAPI.CanExit():
                _App.AppAPI.Exit()
                _App.AppAPI = None
            raise PermissionError()
        if setcontext:
            self.set_login_context(site=site, team=team, extension=extension)
        handler = DefaultEventHandler()
        self.set_event_handler(handler)
        _App.AppAPI.SetWindowsVisible(True)

    def can_attach(self):
        """
        Whether the API can attach to an uAgent instance or not.
        :return: True if the API can attach. False whatelse.
        """
        if _App.AppAPI is None:
            _App.AppAPI = comtypes.client.CreateObject(appapi)
        return _App.AppAPI.CanAttach()

    def attach(self, username=None, password=None):
        """
        Attaches API to opened uAgent instance.
        :param username: Username to log in with.
        :param password: Password to log in with.
        :return:
        """
        global API, AppAPI
        if username is None:
            username = getpass.getuser()
        if _App.AppAPI is None:
            _App.AppAPI = comtypes.client.CreateObject(appapi)
        if _App.AppAPI.CanAttach():
            _App.API = _App.AppAPI.Attach(username, password)
        else:
            if _App.API.CanExit():
                _App.API.Exit()
                _App.API = None
        handler = DefaultEventHandler()
        self.set_event_handler(handler)

    def login(self, *, instance=None, username=None, password=None, secureconnection=None,
              setcontext=True, site=None, team=None, extension=None):
        """
        Logs in or attaches to a new uAgent instance.
        :param instance: Network path of Altitude instance.
        :param username: Username to log in with.
        :param password: Password to log in with.
        :param secureconnection: Whether to use secure connection or not. False by default.
        :param setcontext: Whether to set context or not at log in. If True, site and extension might be indicated.
                           True by default.
        :param site: Site name to log in to.
        :param team: Team name to log in to.
        :param extension: Extension in site to log telephony to.
        :return: None.
        """
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
        _App.API = comtypes.client.CreateObject(api)
        _App.AppAPI = comtypes.client.CreateObject(appapi)
        if _App.AppAPI.CanAttach():
            _App.API = _App.AppAPI.Attach(username, password)
        else:
            _App.AppAPI.Exit()
            _App.AppAPI = None
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
                _App.API.Login(instance, username, password, secureconnection)
            except:
                raise

            # Si está definido el contexto, lo implicamos directamente.
            # ¿Para qué esperar?
            if setcontext:
                self.set_login_context(site=site, team=team, extension=extension)
        handler = DefaultEventHandler()
        self.set_event_handler(handler)

    def logout(self):
        """
        Logs out on uAgent Instance.
        :return: None.
        """
        _App.API.CleanUpAgent(True)
        if _App.AppAPI and _App.AppAPI.CanExit():
            _App.AppAPI.Exit()

    def execute(self, sql, bind_list=tuple()):
        """
        Executes specified sql.
        :param sql: Sql strig to execute.
        :param bind_list: list of data to replace "?" in sql with.
        :return: SqlParser instance with executed query.
        """
        sql = SqlParser(sql, bind_list, api=_App.API)
        # self.parsers.append(sql)
        return sql

        #############################
        #                            #
        # App: Methods            2  #
        #                            #
        #############################

    def search_contacts(self, campaign, sqlwhere):  # Bullshit
        """
        Returns a list with 50 contacts per item
        :param campaign: Campaign name to seach contacts in.
        :param sqlwhere: Condition to apply to query in sql format.
        :return: Generator with 50 contacts for yield instance.
        """
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
            _App.API.CloseContactsCursor(campaign, cursor)
            yield finalcontacts

    @daemonize
    def set_event_handler(self, handler):
        """
        Setas event handler. Exeuted in a daemon.
        :param handler: Handler to set to.
        :return: None.
        """
        self._event_handler = comtypes.client.GetEvents(_App.API, handler, uAgentAPIEvents)
        while True:
            try:
                comtypes.client.PumpEvents(0.5)
            except WindowsError as details:
                if details.args[3] != -2147417835:  # timeout expired
                    pass
                else:
                    raise

    def set_login_context(self, site=None, team=None, extension=None):
        """
        Sets login context.
        :param site: Site to connect to.
        :param team: Team to connect to.
        :param extension: Extension in site to connect to.
        :return: None.
        """
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
        _App.API.SetLoginContext(site, team, extension)

    def set_not_ready(self, reason):
        """
        Sets aux not ready reason to every opened campaign.
        :param reason: Aux reason to set to.
        :return: None.
        """
        _App.API.GlobalSetNotReady(_App.API.GetNotReadyReasons().Index(reason))

    # Raw uAgentAPI wrapper
    def GetPhoneInfo(self, sessionID):
        """
        Returns the telephony data of a session.
        :param sessionID: SessionID of opened session
        :return: PhoneInfo
        """
        phone = _App.API.GetPhoneInfo(sessionID)
        phone_data = {"Acd": phone.Acd,
                      "CallKey": phone.CallKey,
                      "DialedNumber": phone.DialedNumber,
                      "Dnis": phone.Dnis,
                      "IsRecording": phone.IsRecording,
                      "Number": phone.Number,
                      "PrimaryParticipants": phone.PrimaryParticipants,
                      "SecondaryParticipants": phone.SecondaryParticipants,
                      "State": phone.State}
        for key in ("PrimaryParticipants", "SecondaryParticipants"):
            items = list()
            for x in range(phone_data[key].Count):
                items.append({"Name": phone_data[key].Index(x).Name,
                              "Number": phone_data[key].Index(x).Number,
                              "Type": phone_data[key].Index(x).Type})
            phone_data[key] = items
        return phone_data

    def GetSessionInfo(self, sessionID):
        """
        Returns an object of type dict with the data associated to a session.
        :param sessionID: SessionID of opened session
        :return: SessionInfo
        """
        session = _App.API.GetSessionInfo(sessionID)
        return {"Campaign": session.Campaign,
                "HasContactLoaded": session.HasContactLoaded,
                "HasDataTransaction": session.HasDataTransaction,
                "HasVoice": session.HasVoice,
                "IsAlerting": session.IsAlerting,
                "IsDelivered": session.IsDelivered,
                "IsRecording": session.IsRecording,
                "PhoneState": session.PhoneState,
                "ScriptOnAlerting": session.ScriptOnAlerting,
                "SessionType": session.SessionType
                }

    def PhoneSendDigits(self, sessionID, digits):
        """
        Sends Digits as DTMF Tones
        :param digits: String to send
        :return: None
        """
        _App.API.PhoneSendDigits(sessionID, digits)

    def SendData(self, agent, campaign, messages):
        """
        Sends data to specified agent logged in specified campaign.
        :param agent: Agent name to send data to.
        :param campaign: Campaign name in which agent is logged in.
        :param messages: Dictionary of messages to send to agent.
        :return: None.
        """
        assert isinstance(messages, dict)
        attributes = comtypes.client.CreateObject(UAAttributeList)
        for key in messages:
            """
            attr = comtypes.client.CreateObject(UAAttribute)
            attr.Name = key
            attr.Value = messages[key]
            attributes.Add(attr)""" # Curiously, it wasn't needed.
            attributes.add(key, messages[key]) # It does everything by itself.
        _App.API.SendData(agent, campaign, attributes)


            ################    ####  ###  #      ###   ###   ###   ####  ####  ###
            #               #  #     #   # #     #   # #   # #   # #     #     #
        #  #
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

    def __init__(self, sql, bind_list=tuple(), *, app=None, api=None):
        """
        Initizes SqlParser.
        :param sql: Sql to parse to.
        :param bind_list: List of items to replace "?" with.
        :param app: _App instance. None by default.
        :param api: COM uAgentAPI instance. None by default.
        """
        if api:
            self.API = api
        elif app:
            self.API = app.API
        else:
            self.API = _App.API
        self._freezed = False
        self._count = int()
        self._tables = list()
        self._columns = list()
        self._types = dict()
        self._where = str()
        self._items = dict()
        self._index = dict()
        self._bind_list = bind_list
        self._iter_index = int()
        if not " drop " in sql:
            self.cursorSQL = -1
            self.lastquery = ""
            self._sql = SqlParser.parse_sql(sql, self._bind_list)
            if self._sql.strip()[0:7].lower() == "select ":
                self._count = self.get_count()
            self.execute(self.sql)

        else:
            raise Exception("No seas malo")

    def __del__(self):
        """
        Deletes SqlParser cleanly.
        :return: None.
        """
        if not self.freezed:
            self.close_cursor()

            #############################
            #                            #
            # SqlParser: __getitem__     #
            #            __iter__        #
            #            __getattr__     #
            #                            #
            #############################

    def get_item(self, key):
        """
        Gets iten with key index.
        :param key: index of data.
        :return: Item instance with required data.
        """
        if key < int(self._count):
            try:
                page = int(key / MAX_ROWS) + 1
                subkey = key - ((page - 1) * MAX_ROWS)
                if page not in self.items:
                    data = self.fetch_page(page)
                    # item = Item(data, self.columns, subkey)
                print(key)
                nndata = dict(self.items[page].set_row(key))
                final_data = dict()
                for kkey in nndata:
                    final_data[kkey] = self._types[kkey](nndata[kkey])
                return final_data
            except KeyError:
                return []
        else:
            raise IndexError("EOL")

    def __getitem__(self, key):
        """
        Gets iten with key index.
        :param key: index of data.
        :return: Item instance with required data.
        """
        if key < int(self._count):
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
        """
        Returns iterator.
        :return: self.
        """
        return self

    def __next__(self):
        """
        Returns next item in iterator.
        :return: next item in iterator.
        """
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
        """
        I don't remember what the hell does it, but it may be documented anyway.
        :param attribute: Attribute to get.
        :return: Required attribute.
        """
        if attribute in self.index:
            class Index:
                def __init__(self, parser, index):
                    self.parser = parser
                    self.index = index
                def __call__(self, value, method=QueryMatchType.NoValue):
                    return self.parser.get_index(self.index, value, method)
            return Index(self, attribute)
        else:
            super().__getattribute__(attribute)


            #############################
            #                            #
            # SqlParser: Properties      #
            #                            #
            #############################

    @property
    def columns(self):
        """
        List of column names in query.
        """
        return self._columns

    @property
    def count(self):
        """
        Amount of registries in query.
        """
        return int(self._count)

    @property
    def freezed(self):
        """
        Whether if connection is closed or not.
        """
        return self._freezed

    @property
    def index(self):
        """
        Index number of current registry.
        """
        return self._index

    @property
    def items(self):
        """
        List of fetched items.
        """
        return self._items

    @property
    def pages(self):
        """
        Number of available pages.
        """
        real = self.count / MAX_ROWS
        part = real - int(real)
        return int(real) + (part > 0 and 1 or 0)

    @property
    def sql(self):
        """
        Sql query string.
        """
        return self._sql

    @property
    def tables(self):
        """
        List of table names queried.
        """
        return self._tables

    @property
    def where(self):
        """
        Where clause.
        """
        return self._where

        #Properties getters and doers fo manager
    def do_count(self):
        """
        Counts avaliable registries in query.
        :return: Number of available registries.
        """
        return int(self._count)

    def keys(self):
        """
        Returns avalizable columns. It mimics dict.keys method.
        :return: List of available columns.
        """
        return self._columns

    def is_freezed(self):
        """
        Returns whether connection is opened or not.
        :return: True if connection closed and all registries fetched.
        """
        return self._freezed

    def get_current_index(self):
        """
        Returns current registry index.
        :return: Current registry index.
        """
        return self._index

    def total_pages(self):
        """
        Returns amount of total pages availables.
        :return: Amount of total pages availables.
        """
        return self.pages

    def get_sql(self):
        """
        Returns SQL query.
        :return: SQL query.
        """
        return self._sql

    def get_table_names(self):
        """
        Returns list of queried table names.
        :return: List of queried table names.
        """
        return self._tables

    def get_where_clause(self):
        """
        Returns where clause.
        :return: Where clause.
        """
        return self._where

        #############################
        #                            #
        # SqlParser: Static Methods  #
        #                            #
        #############################

    @staticmethod
    def get_tables(sql):
        """
        Gets affected table names from sql query.
        :param sql: SQL query string to get table names from.
        :return: List of queried table names.
        """
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
        """
        Gets where clause from sql string.
        :param sql: SQL query to get where clause from.
        :return: list with where clause.
        """
        if " where " in sql:
            if " group by " in sql:
                return re.findall(r"(?<=where )([\w+ \'=<>()\.\-#:%]+)(group by [\w+ ,]+)?", sql.lower())[0]
            else:
                return re.findall(r"(?<=where )([\w+ \'=<>()\.\-#:%]+)(order by [\w+ ,]+)?", sql.lower())[0]
        else:
            return ["", ""]

    @staticmethod
    def parse_sql(sql, bind_list=tuple()):
        """
        Parses specified sql string.
        :param sql: SQL query to parse.
        :param bind_list: List of items to replace "?" with.
        :return: parsed sql with securely replaced items.
        """
        sql = sql.replace("\r\n", " ")
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
        """
        Closes SQL cursor.
        :raise: Exception in case cursor is already closed.
        """
        if not self.freezed and self.cursorSQL != -1:
            self.API.CloseSQLCursor(self.cursorSQL)
            # print("Closed {} cursor".format(str(self.cursorSQL)))
        elif self.freezed:
            raise Exception("No se puede cerrar un cursor ya cerrado.")

    def execute(self, sql, bind_list=()):
        """
        Executes given sql. Obsolete.
        :param sql: Sql query to ewxecute.
        :param bind_list: List of items to replace "?" with.
        :return: None
        """
        if not self.freezed:
            for x in range(2):
                try:
                    self.cursorSQL = self.API.OpenSqlCursor(self.cursorSQL, sql, self.lastquery)
                    break
                except:
                    # print("Cursor: {}\nSql: {}\nLastQuery: {}".format(self.cursorSQL, sql, self.lastquery))
                    if x == 4: raise
                    time.sleep(1)
            self.lastquery = sql

        else:
            raise Exception("No se puede ejecutar nada con el cursor cerrado.")

    def fetch_page(self, page, save=True):
        """
        Fetches specified page, beggining with 1.
        :param page: number of page to fetch.
        :param save: Whether to save data in page or not.
        :return: Fetched page.
        """
        if not self.freezed and not page in self.items:
            inicial = (page - 1) * MAX_ROWS
            data = self.API.FetchSqlCursor(self.cursorSQL, inicial, MAX_ROWS)
            # print("Fetched {} rows in page {}".format(str(data.rowcount), str(page)))
            if save:
                item = Item(data, self.columns, (page - 1) * MAX_ROWS, self.count, self._types)
                self._items[page] = item
            return data
        elif page in self.items:
            return self.items[page].fetched
        else:
            raise Exception("No se puede hacer peticiones al servidor con el cursor cerrado.")

    def fetch_part(self, save=True):
        """
        Fetches and return next available page.
        :param save: Whether to save fetched page or not.
        :return: Generator.
        """
        if not self.freezed:
            page = 1
            total = 1
            inicial = 0
            while total > 0:
                fetched = self.API.FetchSqlCursor(self.cursorSQL, inicial, MAX_ROWS)
                # print("Fetched {} rows in page {}".format(str(fetched.rowcount), str(page)))
                total = fetched.RowCount
                inicial += MAX_ROWS
                if save:
                    item = Item(fetched, self.columns, (page - 1) * MAX_ROWS, self.count, self._types)
                    self._items[page] = item
                page += 1
                yield fetched
        else:
            raise Exception("No se puede hacer peticiones al servidor con el cursor cerrado.")

    def freeze(self, asis=False):
        """
        Freezes the query and closes connection.
        :param asis: Whether to get all the results before freezing or not.
        :return: None
        """
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
        """
        Get queried column names from SQL string.
        :param sql: SQL query to gect columns from.
        :return: Tuple with list of columns and a list of types of every column.
        """
        if not self.freezed:
            sql = sql.replace("\n", " ")
            if not " from" in sql.lower():
                columns = re.findall(r"(?<=select )([\w+ ,()\*\@\[\]\.\-_'<>=/\+]+)", sql.lower())
            else:
                if not "select distinct " in sql.lower()[:32]:
                    columns = re.findall(r"(?<=select )([\w+ ,()\*\@\[\]\.\-_'<>=/\+]+) from", sql.lower())
                else:
                    columns = re.findall(r"(?<=select distinct )([\w+ ,()\*\@\[\]\.\-_'<>=/\+]+) from", sql.lower())
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
            final_types = dict()
            table = str()
            tables = re.findall(r"[from|join] ([\w\._\-]+) as ([\w\._\-]+)", sql)
            tables = dict([(table[1], table[0]) for table in tables])
            if "select " in sql:
                for column in columns:
                    if "*" in column:
                        table = re.findall(r"([\w]+)\.\*", column.lower()) # Revisar esto
                        if table != list() and table[0] in tables:
                            ttable = tables[table[0]]
                        elif table != list():
                            ttable = table
                        else:
                            ttable = re.findall(r"[from|join] ([\w\._\-]+)", sql)[0]
                        table_columns, types_dict = self.get_columns_names(ttable)
                        for table in table_columns:
                            for col in table_columns[table]:
                                final_columns.append("{}.{}".format(self.tables[table], col))
                                final_types[final_columns[-1]] = types_dict[table][col]
                                '''
                                try:
                                    if col in final_columns: final_columns.append("{}.{}".format(table[0], col))
                                    else: final_columns.append(col)
                                except:
                                    raise
                            '''
                    else:
                        goon = True
                        if " as " in column:
                            try:
                                r_column, column = re.findall(r"([\w\._\-]+) as ([\w\._\-]+)", column)[0]
                            except IndexError:
                                r_column = column.split(" as ")[1]
                                column = r_column
                        else:
                            r_column = column
                        try:
                            table = re.findall(r"([\w]+)\.", r_column.lower())[0]
                        except IndexError:
                            try:
                                table = re.findall(r"[from|join] ([\w\._\-]+) ", sql)[0]
                            except IndexError:
                                goon = False
                        if goon is True:
                            if table in tables:
                                ttable = tables[table]
                            else:
                                ttable = table
                            if "dbo" in table:
                                db, ttable = re.findall("([\w]+.)dbo.([\w]+)", table)[0]
                            else:
                                db = str()
                            if "information_schema" not in sql:
                                ccolumn = "." in r_column and r_column[len(table)+1:] or r_column
                                ssql = "select data_type from {}information_schema.columns " \
                                       "where table_name='{}' and column_name='{}'".format(db, ttable, ccolumn)
                                    # print("Sql: {}".format(sql))
                                sqlnames = SqlParser(ssql, api=self.API)
                                print(db, ttable, ccolumn)
                                pagina = sqlnames.fetch_page(1, False)
                                try:
                                    final_types[column] = pagina.Index(0, 0) in DATA_TYPES and DATA_TYPES[pagina.Index(0, 0)] or str
                                except comtypes.COMError:
                                    final_types[column] = str
                            else:
                                final_types[column] = str
                            final_columns.append(column)
                        else:
                            final_types[column] = str
                            final_columns.append(column)

                self._sql = self._sql.replace("*", ", ".join(final_columns))
                if final_columns != list():
                    # self._sql = self._sql.replace("*", ", {}.".format(table).join(final_columns))
                    self._sql = self._sql.replace("*.{}".format(table), ", ".join(final_columns))
                return (final_columns, final_types)
        else:
            return (self.columns, self._types)

    def get_columns_names(self, table_name=None):
        """
        Get names of queried columns for every queried table.
        :param table_name: List of queried tables. If None, get self.tables instead. None by default.
        :return: Tuple with list of columns and a list of types of every column.
        """
        if table_name == None:
            table_name = [table for table in self.tables]
        final = dict()
        types = dict()
        if isinstance(table_name, str):
            table_name = [table_name]
        if not self.freezed:
            for table in table_name:
                # print("Table: {}".format(table))
                if not "information_schema" in table:
                    db = str()
                    tabled = table
                    if "dbo" in table:
                        db, tabled = re.findall("([\w]+.)dbo.([\w]+)", table)[0]
                    print(db, tabled)
                    sql = "select column_name, data_type from {}information_schema.columns where table_name='{}'".format(db,
                                                                                                              tabled)
                    # print("Sql: {}".format(sql))
                    sqlnames = SqlParser(sql, api=self.API)
                    names = list()
                    typping = dict()
                    for x in range(sqlnames.pages):
                        pagina = sqlnames.fetch_page(x + 1, False)
                        for y in range(MAX_ROWS):
                            try:
                                names.append(pagina.Index(y, 0))
                                typo = pagina.Index(y, 1) in DATA_TYPES and DATA_TYPES[pagina.Index(y, 1)] or str
                                typping[names[-1]] = typo
                            except:
                                break
                    if table not in final:
                        final[table] = list()
                    final[table].extend(names)
                    types[table] = typping
            # print("Total Columnas: {}".format(len(final)))
            return (final, types)
        else:
            return (self.columns, self._types)

    def get_count(self):
        """
        Converts and executes a count sql query because api connection with sqlserver limitations.
        :return: Number of queried registries.
        """
        if (not self.freezed and self.sql != str() and
                    "insert into " not in self.sql.lower() and
                    "update " not in self.sql.lower()):
            self._tables = SqlParser.get_tables(self.sql)
            self._columns, self._types = self.get_columns(self.sql)
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

    def get_index(self, index, value, method=QueryMatchType.NoValue):
        """
        Gets specified item given index position and its value.
        :param index: Index position of registry.
        :param value: Value of registry.
        :param method: Kind of match type. Not used. Why?
        :return: "GetterIndex" instance with given data.
        """
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
        """
        Sets whatever I supposed it should set when I wrote all of these.
        :param index: Index to set.
        :return: Nonw.
        """
        if self.freezed:
            if index in self.columns and not index in self.index:
                self.index[index] = dict()
                for ind, item in enumerate(self):
                    key = item.get_column(index)
                    if key not in self.index[index]:
                        self.index[index][key] = set()
                    self.index[index][key].add(ind)

    def primary_key(self, table):
        """
        Gets the primary key of the specified table.
        :param table: Table to get the primary key from.
        :return: Primary key column name.
        """
        sql = "select i.name as index_name, COL_NAME(ic.object_id, ic.column_id) as column_name, ic.index_column_id, ic.key_ordinal, ic.is_included_column from sys.indexes as i inner join sys.index_columns as ic on i.object_id = ic.object_id and i.index_id = ic.index_id where i.object_id = OBJECT_ID('{}') and i.is_primary_key=1".format(
            table)
        key = SqlParser(sql, api=self.API)
        return key[0].column_name

    def unique_key(self, table):
        """
        Gets the unique key of the specified table.
        :param table: Table to get the unique key from.
        :return: Unique key column name.
        """
        sql = "select i.name as index_name, COL_NAME(ic.object_id, ic.column_id) as column_name, ic.index_column_id, ic.key_ordinal, ic.is_included_column from sys.indexes as i inner join sys.index_columns as ic on i.object_id = ic.object_id and i.index_id = ic.index_id where i.object_id = OBJECT_ID('{}') and i.is_unique=1".format(
            table)
        key = SqlParser(sql, api=self.API)
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
        """
        Initializes Config object.
        :param path: Base path for user-app folder.
        :param pathclass: Class to instantiate path.
        :param kwargs: kwargs for self.initialize method.
        """
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
        """
        Server path in config.
        """
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
        """
        Initializes the config file with the most basic configurations
        :param instance: Default Altitude instance network path.
        :param username: Username to uAgent logging in.
        :param password: Password to uAgent logging in.
        :param secureconnection: Whether to use secure connection or not.
        :param site: Site name to connect telephony to.
        :param team: Team name to connect to.
        :param extension: Extension in site to connect to.
        :param kwargs: Other kwargs.
        :return: None
        """
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
        """
        Rereads configuration from ini file.
        :return: None
        """
        self.read(self.path.config)

    def save(self):
        """
        Saves da file, you madafakaaaaaaa.
        :return: None
        """
        if self.path.config:
            with open(self.path.config, "w") as config_file:
                self.write(config_file)

                ################   # #####  ### #   #
                #               #  #   #   #    ## ##
                # Parsed Atom   #  #   #   ###  # # #
                #               #  #   #   #    #   #
                ################  #   #    ### #   #


class Item(dict):
    """
    Dict subclass to represent each registry queried.
    """
    def __init__(self, fetched, column_list, row, total, typos):
        """
        Initializes Item.
        :param fetched: Data fetched.
        :param column_list: List with columns name in order of appearance.
        :param row: Row index of item.
        :param total: Total items in query.
        :param typos: List of types of each column.
        """
        dict.__init__(self)
        self.fetched = fetched
        if column_list != list():
            self.column_list = column_list
        else:
            self.column_list = ["Column{}".format(str(x)) for x in range(self.fetched.columncount)]
        self.item_column_list = list(self.column_list)
        #self.column_list = [col.replace(".", "_") for col in self.column_list]
        self.total = total
        self.row = row
        self._iter_index = 0
        self.typos = typos

    def __getitem__(self, item):
        """
        Returns specified column index.
        :param item: index of column to fetch.
        :return: Requested item instantiated with the same self.typos class index.
        """
        return self.typos[item](dict.__getitem__(self, item))

    def __getattr__(self, attribute):
        """
        Gets column as attribute.
        :param attribute: Column name. It can be a partial name as well.
        :return: Asked item in Item.
        """
        if attribute in self:
            return self[attribute]
        else:
            for item in self:
                if attribute.lower() == item[0 - len(attribute):].lower().replace(".", "_"):
                    return self[item]
            raise AttributeError()

    @property
    def row(self):
        """
        Index row in query.
        """
        return self._row

    @row.setter
    def row(self, value):
        """
        Sets this index row in query.
        :param value: Index row in query.
        :return: None.
        :raise: IndexError if row greater than total items.
        """
        if self.get_subrow(value) >= self.total:
            raise IndexError
        else:
            self._row = value

    def get_column(self, column):
        """
        Gets item value by column name.
        :param column: Name of column to fetch item.
        :return: Value of item from given column.
        """
        return self.__getattr__(column)

    def get_subrow(self, row): #DEPRECATED
        """
        Returns in page index.
        :param row: Number of row to get index in page.
        :return: In page index.
        """
        return row - MAX_ROWS * int(row / MAX_ROWS)

    def get_row(self, row, column): #DEPRECATED
        """
        Returns data from given row and column.
        :param row: Index row.
        :param column: Column name.
        :return: Data from given row and column.
        """
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
        """
        Sets specified row as current row.
        :param row: Index row.
        :return: self.
        """
        self.row = row
        subrow = row - MAX_ROWS * int(row / MAX_ROWS)
        data = [self.fetched.Index(subrow, column)
                for column in range(self.fetched.columncount)]
        self.clear()
        self.update(zip(self.item_column_list, data))
        return self

    def get_typos(self):
        """
        Gets list of class types callables for each column.
        :return: List of class types callables for each column.
        """
        return self.typos

        ##################
        #                 #
        # Events Handler  #
        #                 #
        ##################


class DefaultEventHandler(object):
    '''
    Default Event Handler.
    '''
    pipes_in = list()

    @classmethod
    def send_pipe(cls, data):
        """
        Sends data to all pipes in cls.pipes_in
        :param data: Data to send.
        :return: None.
        """
        to_del = list()
        for index, pipe in enumerate(cls.pipes_in):
            try:
                pipe.send(data)
            except BrokenPipeError:
                to_del.append(index)
        for index in to_del:
            del(cls.pipe_in[index])

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
        _App.INSTANCE.session_id = sessionID
        now = datetime.datetime.now()
        print(sessionID)
        phone_data = _App.INSTANCE.GetPhoneInfo(sessionID)
        session_info = _App.INSTANCE.GetSessionInfo(sessionID)
        final = {"SesionPhoneEvent": {"DestinationNumber": data.DestinationNumber,
                                      "DestinationUserName": data.DestinationUserName,
                                      "IsRecording": data.IsRecording,
                                      "PhoneState": data.PhoneState,
                                      "RecordingTerminationReason": data.RecordingTerminationReason,
                                      "Time": now,
                                      "PhoneInfo": phone_data,
                                      "SessionInfo": session_info}}
        DefaultEventHandler.send_pipe(("SessionPhoneEvent", [sessionID, final]))

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

class Manager(BaseManager):
    """
    Base client manager class
    """
    pass
Manager.register("app")
Manager.register("user")
Manager.register("sqlparser")
Manager.register("queue")
Manager.register("shutdown")
Manager.register("pipe")

def get_manager():
    """
    Searches for the opened manager by active windows user
    :return: tuple with client manager and active port
    """
    port = PORT
    while True:
        try:
            manager = Manager(address=(HOST, port), authkey=b"uAgentAPI.Wrapper")
            manager.connect()
        except ConnectionRefusedError:
            return (None, port)
        else:
            if str(manager.user()) == "'" + os.environ["USERNAME"] + "'":
                return (manager, port)
            else:
                port += 1
                continue


def manager_open():
    """
    Opens the manager in other command instance
    :return:
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Wrapper.py")
    if not os.path.exists(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Wrapper.pyc")
    args = [sys.executable] + [path]
    new_environ = os.environ.copy()
    subprocess.Popen(args, env=new_environ,
                     #creationflags=0x08000000) # To hide console
                     creationflags=subprocess.CREATE_NEW_CONSOLE) # to watch console
    time.sleep(1)


class App():
    """
    Client App class to use with manager
    """
    class SqlParser(list):
        """
        App.SqlParser class to use with manager
        """
        class Item(dict):
            """
            App.SqlParserItem class to use with manager
            """
            def __getattribute__(self, attribute):
                return dict.__getattribute__(self, attribute)

            def __getattr__(self, attribute):
                """
                Searches for attribute in manager and returns it
                :param attribute: name of the attribute to look for
                :return: Attribute in manager
                """
                if attribute in self:
                    return self[attribute]
                else:
                    keys = list(self.keys())
                    for item in keys:
                        if item.endswith(attribute):
                            return self[item]
                raise AttributeError()

        def __init__(self, parser):
            """
            Initializes App.SqlParser
            :param parser: parser got from sql executeing in remote manager
            """
            list.__init__(self)
            self._parser = parser
            self._iter_index = int()

        def __getitem__(self, item):
            """
            Gets an item in parer and returns it
            :param item: item index to get
            :return: Item found by item index.
            """
            if isinstance(item, int):
                if item < 0:
                    item = len(self)+item
                return App.SqlParser.Item(self._parser.get_item(item))
            elif isinstance(item, slice):
                if item.start is not None:
                    init = item.start >= 0 and item.start or len(self)+item.start
                else:
                    init = 0
                if item.stop is not None:
                    end = item.stop >= 0 and item.stop or len(self)+item.stop
                else:
                    end = len(self)-1
                step = item.step and item.step or 1
                return [App.SqlParser.Item(self._parser.get_item(i)) for i in range(init, end, step)]

        def __iter__(self):
            """
            Iterator.
            :return: Iterator.
            """
            self._iter_index = int()
            return self

        def __len__(self):
            """
            Returns the number of registries in Query.
            :return: Number of registries in Query.
            """
            return self._parser.do_count()

        def __next__(self):
            """
            Gives the next item in iterator.
            :return: The next item in iterator.
            """
            index = self._iter_index
            if index < len(self):
                self._iter_index += 1
                return self[index]
            else:
                raise StopIteration()

        def __repr__(self):
            """
            Representation of Query.
            :return: Representation of query.
            """
            return self[:].__repr__()

        @property
        def count(self): #Legacy
            """
            Returns the number of registries in Query.
            """
            return self._parser.do_count()

        @property
        def columns(self):
            """
            Gives a list of column names in the query.
            """
            return self._parser.keys()

        @property
        def freezed(self):
            """
            Returns whether or not the connectionm is closed and data fetched.
            """
            return self._parser.is_freezed()

        @property
        def index(self):
            """
            Returns index of current registry.
            """
            return self._parser.get_current_index()

        @property
        def pages(self):
            """
            Returns the total of pages available.
            """
            return self._parser.total_pages()

        @property
        def sql(self):
            """
            Returns the sql string query.
            """
            return self._parser.get_sql()

        @property
        def tables(self):
            """
            Returns a list of queried tables.
            """
            return self._parser.get_table_names()

        @property
        def where(self):
            """
            Returns the where clause.
            """
            return self._parser.get_where_clause()

    def __init__(self, path=None, *, pathclass=Path):
        """
        Initializes the local App.
        :param path: Path to local configuration.
        :param pathclass: Class to instantiate path.
        """
        self.config = Config(path, pathclass) # We create a 'Config' object to access the configuration
        for x in range(5):
            try:
                self._manager, self._port = get_manager()
                self._app = self._manager.app()
            except AttributeError:
                time.sleep(x+0.5)
            else:
                break

    def __getattribute__(self, attribute):
        """
        Tries to get attribute in manager and return it.
        :param attribute: Attribute to search for.
        :return: Value of attribute looked for.
        """
        try:
            return object.__getattribute__(self, attribute)
        except (ConnectionRefusedError, ConnectionResetError):
            manager_open()
            time.sleep(2)
            return object.__getattribute__(self, attribute)

    def __getattr__(self, attribute):
        """
        Tries to get attribute in manager and return it.
        :param attribute: Attribute to search for.
        :return: Value of attribute looked for.
        """
        try:
            return object.__getattribute__(self, "_app").__getattribute__(attribute)
        except (ConnectionRefusedError, ConnectionResetError):
            manager_open()
            time.sleep(2)
            return object.__getattribute__(self, "_app").__getattribute__(attribute)

    @property
    def campaigns(self):
        """
        List of available campaigns.
        """
        return self._app.get_campaigns_name()

    @property
    def campaigns_data(self):
        """
        List of available campaigns data.
        """
        return self._app.get_campaigns()

    @property
    def historic_events(self):
        """
        List of events.
        """
        return self._app.get_historic_events()

    @property
    def historic_phones(self):
        """
        List of outbounding and inbounding phones
        """
        return self._app.get_historic_phones()

    @property
    def is_logged(self):
        """
        Username if loggedin, False instead.
        """
        return self._app.get_is_logged()

    @property
    def last_phone(self):
        """
        Last phone inbounded or outbounded.
        """
        return self._app.get_last_phone()

    @staticmethod
    def open_server():
        """
        Opens the manager if closed.
        """
        manager_open()

    def execute(self, sql, bind_list=tuple()):
        """
        Execute requested sql string query.
        :param sql: SQL string query.
        :param bind_list: List of data to replace "?" with.
        :return: App.SqlParser with sql parsed data.
        """
        try:
            data = self._manager.sqlparser(sql, bind_list)
            return App.SqlParser(data)
        except ConnectionRefusedError:
            manager_open()
            return self.execute(sql, bind_list)

    def shutdown(self):
        """
        Shutdowns the remote server.
        :return: None
        """
        try:
            self._manager.shutdown()
        except ConnectionResetError:
            pass # We expect this error if everything goes OK

    def set_handler(self, handler):
        """
        Sets a callable as handler to handle data returned by pipe.
        :param handler:
        :return:
        """
        assert hasattr(handler, "__call__")
        self.handler = handler
        Thread(target=self.handler, args=[self._manager.pipe()], daemon=True).start()


if __name__ == "__main__":
    user = os.environ["USERNAME"]
    manager, port = get_manager()
    if manager is None:
        manager_queue = queue.Queue()
        app = _App()
        class ProcessManager(BaseManager):
            pass
        def new_pipe():
            pipe_out, pipe_in = Pipe(False)
            DefaultEventHandler.pipes_in.append(pipe_in)
            return pipe_out
        ProcessManager.register("app", lambda app=app: app)
        #ProcessManager.register("api", lambda app=app: app.API)
        ProcessManager.register("user", lambda user=user: user)
        ProcessManager.register("sqlparser", lambda *args, app=app: SqlParser(*args, app=app))
        ProcessManager.register("queue", lambda manager_queue=manager_queue: manager_queue)
        ProcessManager.register("shutdown", lambda manager_queue=manager_queue: manager_queue.put("Shutdown"))
        ProcessManager.register("pipe", new_pipe)
        processmanager = ProcessManager(address=("0.0.0.0", port), authkey=b"uAgentAPI.Wrapper")
        server = processmanager.get_server()
        @daemonize
        def start():
            server.serve_forever()
        start()
        try:
            from Updater import updater
        except ImportError:
            updater = None
        if updater is not None:
            updater.register(get_manager()[0])
        while True:
            try:
                data = manager_queue.get() # Don't know if a Timeout is imperative now...
            except queue.Empty:
                break
            else:
                if data == "Shutdown":
                    app.logout()
                    break
                else:
                    pass #TODO
        server.server_close()
else:
    manager_open()

