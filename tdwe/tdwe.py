import pandas as pd
from suds.client import Client

DEFAULT_URLS = {
    "WSDL": "http://dataworks.thomson.com/Dataworks/Enterprise/1.0/webserviceclient.asmx?WSDL",
    "FUNCTIONS": "http://extranet.datastream.com/dfosamples/functions.xml",
    "DATATYPES": "http://extranet.datastream.com/dfosamples/datatypes.xml",
}

STATUS_TYPE_CONNECTED = 'Connected'
STATUS_TYPE_STALE = 'Stale'
STATUS_TYPE_FAILURE = 'Failure'
STATUS_TYPE_PENDING = 'Pending'

STATUS_CODE_OK = 0
STATUS_CODE_DISCONNECTED = 1
STATUS_CODE_SOURCE_FAULT = 2
STATUS_CODE_NETWORK_FAULT = 3
STATUS_CODE_ACCESS_DENIED = 4
STATUS_CODE_NO_SUCH_ITEM = 5
STATUS_CODE_BLOCKING_TIMEOUT = 11
STATUS_CODE_INTERNAL = 12


class TDWE(object):
    def __init__(self, username, password, urls=None):
        self.urls = DEFAULT_URLS if urls is None else urls
        self.client = Client(self.urls["WSDL"], username=username, password=password)
        self.userdata = self.client.factory.create("UserData")
        self.userdata.Username = username
        self.userdata.Password = password
    
    def _get_value(self, f):
        if "Value" in f:
            return f["Value"]
        if "ArrayValue" in f:
            # list returns ("anyType": <list>) tuples
            return list(f["ArrayValue"])[0][1]

    def system_info(self):
        resp = self.client.service.SystemInfo()
        fields = resp[0]
        return dict((f["Name"], self._get_value(f)) for f in fields)

    def sources(self):
        resp = self.client.service.Sources(self.userdata, 0)
        sources = resp[0]
        return list(s.Name for s in sources)

    def functions(self):
        pass

    def datatypes(self):
        pass

    def request_single(self, instrument, source="Datastream", 
                       fields=[], options=None, symbol_set=None, tag=None):
        rd = self.client.factory.create("RequestData")
        rd.Source = source
        rd.Instrument = instrument
        for f in fields:
            rd.Fields = self.client.factory.create("ArrayOfString")
            rd.Fields.string = fields
        rd.SymbolSet = symbol_set
        rd.Options = options
        rd.Tag = tag

        return self.client.service.RequestRecord(self.userdata, rd, 1)

    def request_many(self, instruments, source="Datastream", 
                     fields=[], options=None, symbol_set=None, tag=None):
        
        req = self.client.factory.create("ArrayOfRequestData")
        req.RequestData = []
        for i in instruments:
            rd = self.client.factory.create("RequestData")
            rd.Source = source
            rd.Instrument = i
            for f in fields:
                rd.Fields = self.client.factory.create("ArrayOfString")
                rd.Fields.string = fields
            rd.SymbolSet = symbol_set
            rd.Options = options
            rd.Tag = tag
            req.RequestData.append(rd)
        
        return self.client.service.RequestRecords(self.userdata, req, 1)

    def request(self, instruments, source="Datastream", 
                fields=[], options=None, symbol_set=None, tag=None):
        if isinstance(instruments, list):
            return self.request_many(instruments, source, fields, options, symbol_set, tag)
        return self.request_single(instruments, source, fields, options, symbol_set, tag)

    def status(self, resp):
        return {"Source": resp["Source"],
                "Instrument": resp["Instrument"],
                "Code": resp["StatusCode"],
                "Type": resp["StatusType"],
                "Message": resp["StatusMessage"]}

    def parse_fields(self, record):
        status = self.status(record)
        if status["Code"] != STATUS_CODE_OK:
            raise AttributeError(status)
        fields = dict((f["Name"], self._get_value(f)) for f in record["Fields"]["Field"])

    def parse_record_to_dataframe(self, record):
        status = self.status(record)
        if status["Code"] != STATUS_CODE_OK:
            raise AttributeError(status)
        fields = dict((f["Name"], self._get_value(f)) for f in record["Fields"]["Field"])
        if "INSTERROR" in fields:
            raise AttributeError({"Source": status["Source"],
                                  "Instrument": status["Instrument"],
                                  "Code": STATUS_CODE_INSTRUMENT_ERROR,
                                  "Type": status["Type"],
                                  "Message": fields["INSTERROR"]})
        meta_data_field_names = ["CCY", "DISPNAME", "FREQUENCY", "SYMBOL", "DATE", "INSTERROR"]
        meta_data = dict((k, v) for k, v in fields.items() if k in meta_data_field_names)
        data_fields = dict((k, v) for k, v in fields.items() if k not in meta_data_field_names)
        return pd.DataFrame(data_fields, index=fields["DATE"]), meta_data
