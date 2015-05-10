import serial
import pymongo
from pymongo import MongoClient

#Conectarse con la mongoDB de Meteor (running)
print('---------------------------------------------------')
print('Conectando a MongoDB...   ')
client = MongoClient('mongodb://127.0.0.1:3001/meteor')
print(client)
databases = str(client.database_names())
print('DBs disponibles: ' + databases)
db = client.meteor
collections = str(db.collection_names())
print('Colecciones disponibles: ' + collections)
print('---------------------------------------------------')

print('Buscando puerto... ')
ser = serial.Serial('/dev/ttyACM0', 9600)
print(ser.name)
print('---------------------------------------------------')

resetedServer = True

print('Capturas?')
times = input()
times = int(times)

while True:
    if times != 0:
        
        #read, decode binary to string and remove whitespace characters (\r\n)
        line = ser.readline().decode('ascii').strip()

        #convert string to python dictionary (key/value pairs)
        key_value = dict(u.split(":") for u in line.split(","))

        #convert flow value (string) to int to manipulate after
        key_value['flow'] = int(key_value['flow'])
            

        #determine node and flow to inc
        if key_value.get('node') == '01':
            nodeToInc = 'Garden'
            
            if resetedServer == True:
                flowToInc = 1
                oldFlow_01 = key_value.get('flow')
            else:
                newFlow_01 = key_value.get('flow')
                flowToInc = newFlow_01 - oldFlow_01
                oldFlow_01 = newFlow_01
                resetedServer = False

        elif key_value.get('node') == '02':
            nodeToInc = 'Shower'

            if resetedServer == True:
                flowToInc = 1
                oldFlow_02 = key_value.get('flow')
            else:
                newFlow_02 = key_value.get('flow')
                flowToInc = newFlow_02 - oldFlow_02
                oldFlow_02 = newFlow_02
                resetedServer = False
            
        
        db.sensors.update({"place":nodeToInc},{"$inc":{"flow":flowToInc}})
        print(key_value)
        print(flowToInc)
            
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
   
       




    
    

