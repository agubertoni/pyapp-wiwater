import serial
import pymongo
from pymongo import MongoClient

# Conectarse con la mongoDB de Meteor (running)
print('---------------------------------------------------')
print('Conectando a MongoDB...   ')

mongoConnected = False
while not mongoConnected:
    try:
        client = MongoClient('mongodb://127.0.0.1:3001/meteor')
        mongoConnected = True
    except pymongo.errors.ConnectionFailure:
        pass

print('mongo running detected!')
print(client)
databases = str(client.database_names())
print('DBs disponibles: ' + databases)
db = client.meteor

collectionsAvailable = False
while not collectionsAvailable:
    try:
        collections = str(db.collection_names())
        collectionsAvailable = True
    except pymongo.errors.AutoReconnect:
        pass

print('Colecciones disponibles: ' + collections)
print('---------------------------------------------------')

# Recovery nodes' address in "nodesInMongo" list
nodesInMongo = []
cursor = db.sensors.find({}, {"node": 1, "_id": 0})
for record in cursor:
    nodesInMongo.append(record["node"])
print('Nodes in mongo:', nodesInMongo)


# Conectarse al puerto serie
serialConnected = False
print('Buscando puerto... ')
while not serialConnected:
    try:
        ser = serial.Serial('/dev/ttyACM0', 9600)
        serialConnected = True
    except serial.SerialException:
        pass

print(ser.name)
print('---------------------------------------------------')

resetedServer = True
oldFlows = {}

print('Capturas?')
times = input()
times = int(times)

while True:
    if times != 0:
        # read, decode binary to string and remove whitespace characters (\r\n)
        line = ser.readline().decode('ascii').strip()

        if line == 'ED Joined':
            print(line)
            times += 1
        else:
            # convert string to python dictionary (key/value pairs)
            key_value = dict(u.split(":") for u in line.split(","))

            # convert flow value (string) to int to manipulate after
            key_value['flow'] = int(key_value['flow'])

            nodeAddrInput = key_value.get('node')
            nodeFlowInput = key_value.get('flow')

            if nodeAddrInput in nodesInMongo:
                old = db.sensors.find_one({"node": nodeAddrInput}, {"ofid": 1, "_id": 0}) #ofid: old flow id
                old = old["ofid"]
                flowToInc = nodeFlowInput - old
                if flowToInc < 0:
                    flowToInc = 1
            else:
                db.sensors.insert({"node": nodeAddrInput, "flow": 0, "ofid": nodeFlowInput})
                nodesInMongo.append(nodeAddrInput)
                flowToInc = 1

            oldFlows[nodeAddrInput] = nodeFlowInput
            db.sensors.update({"node": nodeAddrInput}, {"$inc": {"flow": flowToInc}, "$set": {"ofid": nodeFlowInput}})
            print(key_value)

            times = times - 1

    else:
        print('Repetir? (y/n)')
        kb = input()
        if kb == 'n':
            print('Cerrando puerto ' + ser.name + ' ...')
            ser.close()
            print('Cerrando mongodb... ')
            client.close()
            print('Done!')
            break
        elif kb == 'y':
            print('Capturas?')
            times = input()
            times = int(times)
