from flask import Flask, jsonify, request
import s3fs
import xarray as xr
import numpy as np
import cartopy.crs as ccrs
import numcodecs as ncd 
from cachetools import cached, TTLCache
from pprint import pprint
import os
from flask_jwt_extended import JWTManager, jwt_required

serverApp = Flask(__name__)
data_folder = os.path.join(os.path.dirname(os.getcwd()), 'dataStore/now/')
cache = TTLCache(maxsize=1000, ttl=300)



# Set up Flask-JWT-Extended
serverApp.config['JWT_SECRET_KEY'] = 'hrrr-weather-lawn'
jwt = JWTManager(serverApp)


class ChunkIdFinder:
    fs = s3fs.S3FileSystem(anon=True)
    chunk_index = xr.open_zarr(s3fs.S3Map("s3://hrrrzarr/grid/HRRR_chunk_index.zarr", s3=fs))

    @classmethod
    def getChunkId(cls, lat, long):
     projection = ccrs.LambertConformal(central_longitude=262.5, 
                                        central_latitude=38.5, 
                                        standard_parallels=(38.5, 38.5),
                                        globe=ccrs.Globe(semimajor_axis=6371229, semiminor_axis=6371229))
     x, y = projection.transform_point(long, lat, ccrs.PlateCarree())
    #  pprint(f"x is :{x} and y is {y}")
     nearest_point = cls.chunk_index.sel(x=x, y=y, method="nearest")
     fcst_chunk_id = nearest_point.chunk_id.values
     return fcst_chunk_id, nearest_point
    

# class ChunkIdFinderV2:
#     chunk_index = xr.open_zarr("/grid/HRRR_chunk_index.zarr")
#     ds_transposed = chunk_index['chunk_id'].transpose('x', 'y')
#     chunk_index['chunk_id']= ds_transposed

#     @classmethod
#     def getChunkId(cls, lat, long):
#     #  pprint(f'Latsrv: {lat}, Long: {long}')  # Print lat and long for debugging
#     #  pprint(cls.chunk_index)  # Print the chunk_index dataset for debugging
#      projection = ccrs.LambertConformal(central_longitude=262.5, 
#                                         central_latitude=38.5, 
#                                         standard_parallels=(38.5, 38.5),
#                                         globe=ccrs.Globe(semimajor_axis=6371229, semiminor_axis=6371229))
#      x, y = projection.transform_point(long, lat, ccrs.PlateCarree())
#     #  pprint(f"x is :{x} and y is {y}")
#      nearest_point = cls.chunk_index.sel(x=x, y=y, method="nearest")
#      fcst_chunk_id = nearest_point.chunk_id.values
#      return fcst_chunk_id, nearest_point

@serverApp.before_request
def validate_request():
    required_fields = ['lat', 'long']  # replace with your actual fields
    if not request.json:
        return jsonify({'error': 'Missing JSON in request body'}), 400
    for field in required_fields:
        if field not in request.json:
            return jsonify({'error': f'Missing field: {field}'}), 400

# Test request
@serverApp.route('/test')
def hello():
    return jsonify({'message': 'Hello, World from mainServer'})


# conversion kelvin to fahrenheit
def kelvin_to_fahrenheit(K):
    F = (K - 273.15) * 1.8 + 32
    return np.round(F, 2)


# Gets the chunk array based on chunk id from the dataStore
def getChunkArr(id,field):
    path = get_latest_folder(data_folder)
    relative_path = os.path.join(path, '1', field, str(id))
    current_directory = os.getcwd()
    url = os.path.join(current_directory, relative_path)
    # pprint(url)
    data = retrieve_data_local(url)  
    return data


# Gets the chunk and returns the nearest data point value
def getChunk(id, nearest_point, field):
    path = get_latest_folder(data_folder)
    relative_path = os.path.join(path, '1', field, str(id))
    current_directory = os.getcwd()
    url = os.path.join(current_directory, relative_path)
    data = retrieve_data_local(url)
    values = data[nearest_point.in_chunk_x, nearest_point.in_chunk_y]
    return values





def get_latest_folder(relative_path):
    current_dir = os.getcwd()
    absolute_path = os.path.abspath(os.path.join(current_dir, relative_path))

    folders = [f for f in os.listdir(absolute_path) if os.path.isdir(os.path.join(absolute_path, f))]
    
    if folders:
        latest_folder = max(folders, key=lambda f: os.path.getmtime(os.path.join(absolute_path, f)))
        return os.path.join(relative_path, latest_folder)  # Return the path of the latest folder
    
    return None



def retrieve_data_local(url):
    with open(url, 'rb') as compressed_data: # using regular file system
        buffer = ncd.blosc.decompress(compressed_data.read())
        # pprint(buffer)

        dtype = "<f4"
        if "surface/PRES" in url: # surface/PRES is the only variable with a larger data type
            dtype = "<f4"
        # print(dtype)
        chunk = np.frombuffer(buffer, dtype)
        
        entry_size = 150*150
        num_entries = len(chunk)//entry_size

        if num_entries == 1: # analysis file is 2d
            data_array = np.reshape(chunk, (150, 150))
        else:
            data_array = np.reshape(chunk, (num_entries, 150, 150))

    return data_array

# conversion kelvin to fahrenheit
def convert_kelvin_to_fahrenheit(arr):
    return (arr - 273.15) * 1.8 + 32



@serverApp.route('/temperature/now/chunk', methods=['POST'])
@jwt_required()
def getTemperatureChunk():
    data = request.get_json()
    pprint(data)
    lat = data['lat']
    long = data['long']
    chunk_id_finder = ChunkIdFinder()
    chunk_id, nearest_point = chunk_id_finder.getChunkId(lat, long)
    array = getChunkArr(chunk_id,'t2m')
    return jsonify({'chunk': array.tolist()})

@cached(cache)
@serverApp.route('/visibility/now/chunk', methods=['POST'])
@jwt_required()
def getVisibilityChunk():
    data = request.get_json()
    pprint(data)
    lat = data['lat']
    long = data['long']
    chunk_id_finder = ChunkIdFinder()
    chunk_id, nearest_point = chunk_id_finder.getChunkId(lat, long)
    # chunk_id, nearest_point = getChunkId(lat,long)
    # pprint(str(chunk_id))
    print(f"Nearest Point: {nearest_point}")
    array = getChunkArr(chunk_id,'vis')
    return jsonify({'chunk': array.tolist()})


@serverApp.route('/temperature/now', methods=['POST'])
@jwt_required()
def getTemperature():
    data = request.get_json()
    lat = data['lat']
    long = data['long']
    chunk_id_finder = ChunkIdFinder()
    chunk_id, nearest_point = chunk_id_finder.getChunkId(lat, long)
    temperature = getChunk(chunk_id,nearest_point,'t2m')
    tempF = kelvin_to_fahrenheit(temperature)
    return jsonify({'temperature':tempF})

# @serverApp.route('/temperature/now/v2', methods=['POST'])
# def getTemperature1():
#     data = request.get_json()
#     # pprint(request)
#     # pprint(data)
#     lat = data['lat']
#     long = data['long']
#     chunk_id_finder = ChunkIdFinderV2()
#     chunk_id, nearest_point = chunk_id_finder.getChunkId(lat, long)
#     # chunk_id, nearest_point = getChunkId(lat,long)
#     # pprint(str(chunk_id))
#     # print(nearest_point)
#     temperature = getChunk(chunk_id,nearest_point,'t2m')
#     tempF = kelvin_to_fahrenheit(temperature)
#     return jsonify({'temperature':tempF})

@serverApp.route('/visibility/now', methods=['POST'])
@jwt_required()
def getVisibility():
    data = request.get_json()
    pprint(data)
    lat = data['lat']
    long = data['long']
    chunk_id_finder = ChunkIdFinder()
    chunk_id, nearest_point = chunk_id_finder.getChunkId(lat, long)
    visibility = getChunk(chunk_id,nearest_point,'vis')
    serialized_visibility = float(visibility)  # Convert to a float
    return jsonify({'visibility': serialized_visibility})


# @serverApp.route('/visibility/now/v2', methods=['POST'])
# def getVisibility2():
#     data = request.get_json()
#     # pprint(request)
#     pprint(data)
#     lat = data['lat']
#     long = data['long']
#     chunk_id_finder = ChunkIdFinderV2()
#     chunk_id, nearest_point = chunk_id_finder.getChunkId(lat, long)
#     # chunk_id, nearest_point = getChunkId(lat,long)
#     # pprint(str(chunk_id))
#     visibility = getChunk(chunk_id,nearest_point,'vis')
#     serialized_visibility = float(visibility)  # Convert to a float
#     return jsonify({'visibility': serialized_visibility})




if __name__ == '__main__':
 serverApp.run(host="0.0.0.0", port=5000, debug=True)
