from lib import PowerStudio
from datetime import datetime, date
from app.remotewrite import write

def run():
    ps = PowerStudio("https://powerstudio.circutor.com")
    #devices = ps.get_devices()

    #ps.get_devices_variables("EDS")


    #ps.get_devices_variables("0CR04-1")

    #ps.get_values("0CR04-1.AC-Meter")
    #print(ps.split("0CR04-1.DESCRIPTION"))
    #print(ps.split("0CR04-1.DC-Mode 4.VDTTM"))
    #print(ps.split("0CR04-1.EVSE.AC.AC.AE"))

    counters = {
        "0CR04-1.AC-Meter.AE" : 0,
        "0CR04-1.AC-Meter.IE": 0
    }

    records = ps.get_records(["0CR04-1.AC-Meter.AE", "0CR04-1.AC-Meter.IE", "0CR04-1.AC-Meter.VI"], datetime(2022, 1, 1), datetime(2022, 1, 7), period=300)
    for record in records:
        ts = datetime.strptime(record["ts"], "%d%m%Y%H%M%S000")
        for var in record["values"].keys():
            if var in counters:
                counters[var] = counters[var] + float(record["values"][var])
                write(ts, value=counters[var], metric=var, labels={"device": "0CR04-1.AC-Meter", "deviceType":"raption22"})
            else:
                write(ts, value=float(record["values"][var]), metric=var, labels={"device": "0CR04-1.AC-Meter", "deviceType": "raption22"})



if __name__ == "__main__":
    run()