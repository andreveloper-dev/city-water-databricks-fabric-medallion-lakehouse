import json
import uuid
import random
import time
import pytz
from datetime import datetime
from shapely.geometry import Point

customers_df = spark.table(Bronze_Customers)
valid_customer_ids = [row["customer_id"] for row in customers_df.select("customer_id").collect()]
employees_df = spark.table(Bronze_Employes)
valid_employee_ids = [row["employee_id"] for row in employees_df.select("employee_id").collect()]
medellin_spark_df = spark.table(Bronze_Medellin)
gdf_medellin = spark_to_geopandas(df_spark=medellin_spark_df)
polygon = gdf_medellin.geometry.union_all()
minx, miny, maxx, maxy = polygon.bounds

def generar_eventos(max_events=events, intervalo=intervalo):
  horario_colombia = pytz.timezone("America/Bogota")
  for _ in range(max_events):
    generated_order_id = str(uuid.uuid4())
    generated_event_date = datetime.now(horario_colombia).strftime("%d/%m/%Y %H:%M:%S")
    generated_quantity = random.randint(14, 107)
    chosen_customer_id = random.choice(valid_customer_ids)
    chosen_employee_id = random.choice(valid_employee_ids)
    while True:
      point = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
      if polygon.contains(point):
        generated_latitude = point.y
        generated_longitude = point.x
        break 
    event_data = {
    "latitude": generated_latitude,
    "longitude": generated_longitude,
    "date": generated_event_date,
    "customer_id": chosen_customer_id,
    "employee_id": chosen_employee_id,
    "quantity_products": generated_quantity,
    "order_id": generated_order_id
    }
      
    file_path = f"{landing_zone_path}/{generated_order_id}.json" 
    dbutils.fs.put(file_path, json.dumps(event_data), overwrite=True)    
    time.sleep(intervalo)
generar_eventos()