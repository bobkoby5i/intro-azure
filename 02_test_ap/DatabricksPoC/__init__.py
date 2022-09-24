import logging
import json
from databricks import sql
import os

import azure.functions as func




def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    response = {
        "data": [],
        "total_rows": None,
    }
    total_rows = 0

    q2 = "SELECT * FROM default.trips_table LIMIT 3"

    q1 = """SELECT t.VendorID, d.name, SUM(t.passenger_count) as passenger_cnt, count(*) as cnt 
        FROM default.trips_table t, default.dim_vendor_table d 
        where t.VendorID = d.vendorID 
        GROUP BY t.VendorID, d.name order by VendorID, name"""

    q3 = """SELECT t.VendorID, d.name, SUM(t.passenger_count) as passenger_cnt, count(*) as cnt 
        FROM default.trips_view t, default.dim_vendor_view d 
        where t.VendorID = d.vendorID 
        GROUP BY t.VendorID, d.name order by VendorID, name"""        

    with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_TOKEN")) as connection:

        #zamontowac datalake do clustra
        # init scrypty 

        with connection.cursor() as cursor:
            cursor.execute(q1)
            result = cursor.fetchall()

            for row in result:
                #print(row)
                (id, name, passengers, rows) = row
                if rows:
                    total_rows = total_rows + int(rows)
                rec = {
                    "id"          : id,
                    "name"        : name,
                    "passengers"  : passengers,
                    "rows"        : rows
                }
                print(rec)
                response["data"].append(rec)
                #print(row[1])

    response["total_rows"] = total_rows
    print(response)

    func.HttpResponse.mimetype = 'application/json'
    func.HttpResponse.charset = 'utf-8'
    return func.HttpResponse(json.dumps(response))


    return func.HttpResponse(
             json.dumps(response),
             status_code=200
    )
    # return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    # )
