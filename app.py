import pandas as pd
from flask import Flask, jsonify, request
import os, datetime, json,  statistics
from minio import Minio
from minio.error import S3Error
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)

def get_client():
    hostname = os.environ.get('MINIO_HOSTNAME')
    access = os.environ.get('MINIO_ACCESS_KEY')
    secret = os.environ.get('MINIO_SECRET_KEY')
    return Minio(
        hostname,
        access_key = access,
        secret_key = secret,
        secure = False
    )
    
def matching_filters(data):
    today = datetime.today()
    is_image = request.args.get('is_image_exists')
    min_age = request.args.get("min_age", type=int)
    max_age = request.args.get("max_age", type=int)
    if is_image == "True":
        data = [x for x in data if x['img_path'] != False]   
    elif is_image == "False":
        data = [x for x in data if x['img_path'] == False]   
    #min and max
    if None not in (max_age,min_age):
        data = [x for x in data if ((today - datetime.fromtimestamp(x['birthts']/1000.0)).days)/365 < max_age]
        data = [x for x in data if ((today - datetime.fromtimestamp(x['birthts']/1000.0)).days)/365 > min_age]
    elif max_age is not None:
        data = [x for x in data if ((today - datetime.fromtimestamp(x['birthts']/1000.0)).days)/365 < max_age]
    elif min_age is not None:
        data = [x for x in data if ((today - datetime.fromtimestamp(x['birthts']/1000.0)).days)/365 > min_age]
    return(data)

def base_path():
  return './02-src-data'

def csv_from_base_path_for_index(i):
  return '{}/{}.csv'.format(base_path(), i)

def png_from_base_path_for_index(i):
  return '{}/{}.png'.format(base_path(), i)

def output_json_file_path():
  return './all.json'

def transfer_data():
    output = []
    is_photo = []
    for i in range(1000,1100):
        df = pd.read_csv(csv_from_base_path_for_index(i))
        df['user_id'] = i
        output.append(df)
        if os.path.exists(png_from_base_path_for_index(i)):
            is_photo.append(png_from_base_path_for_index(i))
        else:
            is_photo.append(False)

    if not os.path.exists('processed_data'):
        os.mkdir('processed_data')
    output_csv = pd.concat(output, ignore_index=False)
    output_csv['img_path'] = is_photo
    output_csv = pd.DataFrame(output_csv, columns = ["user_id","first_name", " last_name", " birthts", "img_path"])
    output_csv.rename(columns={" birthts": "birthts", " last_name":"last_name"}, inplace = True)
    output_csv.to_csv('./processed_data/all.csv', index = True)
    output_csv.to_json(output_json_file_path(), orient="records", date_format="iso")

@app.route("/data", methods=['GET'])
def display_data():
    try:
        client = get_client()
        obj = client.get_object("users-files","all.json")
        data = json.loads(obj.data.decode("utf8"))
        data = matching_filters(data)
        return jsonify(list(data))  
    except Exception as e:
        print(e, flush = True) 
    finally:
        obj.close()
        obj.release_conn()
    return "have no data uploaded"

@app.route("/data", methods=['POST'])
def reload_data():
    client = get_client()
    found = client.bucket_exists("users-files")
    if not found:
        client.make_bucket("users-files")
    else:
        print("Bucket already exists")
    
    client.fput_object(
        "users-files","all.json", "./all.json"
    )
    print("'/home/htlusty/DE_task_22_v1.1/all.json' is successfully uploaded as "
    "object 'all.json' to bucket 'users-files'")
    return 'Ok'

@app.route("/stats", methods=['GET'])
def average_age(): 
    today = datetime.today()
    client = get_client()
    obj = client.get_object("users-files","all.json")
    data = json.loads(obj.data.decode("utf8"))
    data = matching_filters(data)
    ages = [((today - datetime.fromtimestamp(x['birthts']/1000.0)).days)/365 for x in data]
    mean = {'average_age' : statistics.mean(ages)}
    return jsonify(mean)


if __name__ == '__main__':
    transfer_data()
    sched = BackgroundScheduler(daemon = True)
    sched.add_job(reload_data,'interval', minutes = 60)
    sched.start()
    app.run(debug = True, port = '8080', host = "0.0.0.0")  
    try:
        transfer_data()
    except S3Error as exc:
        print("Error occured.", exc, flush = True)
