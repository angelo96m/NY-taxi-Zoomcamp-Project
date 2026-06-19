#In this class we have both producer and consumer

#Producer part: 
import json
import dataclasses
from dataclasses import dataclass

@dataclass
class Ride: 
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds


def ride_from_row(row): 
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )


def ride_serializer(ride):
    #we transform ride in Python dictionary
    ride_dict = dataclasses.asdict(ride)

    #after the python dict is transfored in a .json file 
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json

#Consumer part: 
def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
    
