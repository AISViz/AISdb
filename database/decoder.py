from pyais import FileReaderStream

# convert 6-bit encoded AIS messages to numpy array format

def rowtest():
    rows = []
    for msg in FileReaderStream('scripts/dfo_project/CCG_AIS_Log_2018-07-10.csv'):
        dmsg = msg.decode()
        content = dmsg.content
        print(f'msg: {dmsg.msg_type}  {content.keys()}')
        #rows.append([content['mmsi'], content['lon'], content['lat'], content['speed'], content['course'], content['heading'], content['second']])
