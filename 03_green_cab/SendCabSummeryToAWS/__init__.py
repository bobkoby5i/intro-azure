import logging
import json
from databricks import sql
import os
import requests
import azure.functions as func

url = 'https://d93ah0g736.execute-api.eu-central-1.amazonaws.com/dev/greencab/trips/summary'
my_headers = {
    "Content-Type": "application/json; charset=utf-8",
    'X-API-Key': os.getenv("AWS_GREEN_CAB_TRIPS_APIKEY")  
    }





def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    response = {
        "data": [],
        "total_trips": None,
    }
    total_trips = 0

    q = "select YYYYMM,  VENDOR_ID, VENDOR_NAME, PASSENGERS, FARE_AMOUNT AS FARE, TIP_AMOUNT AS TIP,  TRIPS, CREATED_AT from gc_trips_summary_table ORDER BY YYYYMM, VENDOR_ID"

    q1 = """SELECT t.VendorID, d.name, SUM(t.passenger_count) as passenger_cnt, count(*) as cnt 
        FROM default.trips_table t, default.dim_vendor_table d 
        where t.VendorID = d.vendorID 
        GROUP BY t.VendorID, d.name order by VendorID, name"""

    q3 = """SELECT t.VendorID, d.name, SUM(t.passenger_count) as passenger_cnt, count(*) as cnt 
        FROM default.trips_view t, default.dim_vendor_view d 
        where t.VendorID = d.vendorID 
        GROUP BY t.VendorID, d.name order by VendorID, name"""  

    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    print(f"Connecting to data brciks  {server_hostname} ...")

    with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_TOKEN")) as connection:

        #zamontowac datalake do clustra
        # init scrypty 

        print(f"connected.")
        print(f"Execute query ...")

        with connection.cursor() as cursor:
            cursor.execute(q)
            result = cursor.fetchall()

            for row in result:
                #print(row)
                (yyyymm, vendor_id, vendor_name, passengers, fare, tip,  trips,  created_at) = row
                if trips:
                    total_trips = total_trips + int(trips)
                rec = {
                    "yyyymm"      : yyyymm,
                    "vendor_id"          : vendor_id,
                    "vendor_name" : vendor_name,
                    "passengers"  : passengers,
                    "trips"       : trips,
                    "fare"        : fare,
                    "tip"         : tip,
                    "adf_created_at"  : created_at.strftime("%Y-%m-%d %H:%M:%S")
                }
                print(rec)
                res = requests.post(url, json = rec, headers=my_headers)
                print("Status Code", res.status_code)
                print("JSON Response ", res)
                response["data"].append(rec)
                #print(row[1])

    response["total_trips"] = total_trips

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')    

    if name:
        response["msg"] = f"Hello, {name}. This HTTP triggered function executed successfully."
    else:
        response["msg"] = "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."

    print(response)


    func.HttpResponse.mimetype = 'application/json'
    func.HttpResponse.charset = 'utf-8'
    return func.HttpResponse(json.dumps(response))






