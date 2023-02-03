import requests
#settings for powerstudio
from xml.dom.minidom import parse, parseString
import xmltodict
from datetime import datetime
import json

import urllib3

urllib3.disable_warnings()

class PowerStudio:
    def __init__(self, url):
        self.url = url

    def split(self, tag):
        if tag.count('.') == 1:
            return tag.split('.')[0], tag.split('.')[1]
        elif tag.count('.') > 1:
            return ".".join(tag.split('.', 2)[0:2]), tag.split('.', 2)[2]

    def get_devices(self):
        r = requests.get(self.url + "/services/user/devices.xml")
        data = xmltodict.parse(r.content)
        devices = []
        for d in data['devices']['id']:
            devices.append(d)
        return devices


    def get_devices_info(self, device_id):
        r = requests.get(
            self.url + "/services/user/deviceInfo.xml?id={device_id}".format(
                device_id=device_id
            ), verify=False
        )
        data = xmltodict.parse(r.content)
        #print(json.dumps(data, indent=2))
        variables = data["devices"]["device"]["var"]
        del data["devices"]["device"]["var"]
        return {
            'info': data["devices"]["device"],
            'vars': variables
        }

    def get_values(self, id):
        r = requests.get(
            self.url + "/services/user/values.xml?id={id}".format(
                id=id
            ), verify=False
        )
        data = xmltodict.parse(r.content)
        #print(json.dumps(data, indent=2))
        values = []
        for v in data['values']['variable']:
            if 'value' in v:
                device, var = self.split(v['id'])
                value = float(v['value'])
                #print(device, '=>', var, '=', value)
                values.append({'device': device, 'var': var, 'value': value})
        return values

    def get_prometheus(self, id):
        device_info = self.get_devices_info(id)
        #print(json.dumps(device_info, indent=2))
        values = self.get_values(id)
        tags_prom = ""
        for t in device_info["info"].keys():
            tags_prom = tags_prom + ",{tag}=\"{value}\"".format(tag=t, value=device_info["info"][t])

        output = ""
        for value in values:
            output = output + "{var}{{device=\"{device}\"{tags}}} {value}\n".format(
                var=value['var'],
                device=value['device'],
                value=value['value'],
                tags=tags_prom
            )
        return output

    def get_json(self, id, ts=datetime.now()):
        device_info = self.get_devices_info(id)
        #print(json.dumps(device_info, indent=2))
        values = self.get_values(id)
        tags = {}
        for t in device_info["info"].keys():
            tags[t] = device_info["info"][t]

        output = {'device': id, 'tags': tags, "values": {}, 'ts': ts.strftime("%Y-%m-%dT%H%M%S")}
        for value in values:
            output["values"][value["var"]] = float(value['value'])
        return output


    def get_records(self, vars, begin, end, period=300):
        var_query = ""
        for var in vars:
            var_query = var_query + "&var={var}".format(var=var)
        url = self.url + "/services/user/records.xml?begin={begin}&end={end}&period={period}{vars_query}".format(
                begin=begin.strftime("%d%m%Y"),
                end=end.strftime("%d%m%Y"),
                period=period,
                vars_query=var_query
            )
        print(url)
        r = requests.get(url, verify=False)
        data = xmltodict.parse(r.content)
        print(json.dumps(data, indent=2))
        values = []
        for record in data["recordGroup"]["record"]:
            r = {"ts": record["dateTime"], "values": {} }
            for field in record["field"]:
                r["values"][field["id"]] = float(field["value"])
            print(r)
            values.append(r)
        return values

