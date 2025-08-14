files = spark.read.format("binaryFile").load(landing_zone_path).select("path")
for row in files.toLocalIterator():
    dbutils.fs.rm(row.path, recurse=True)

def remove_checkpoints(folder_path):
    items = dbutils.fs.ls(folder_path)
    for item in items:
        dbutils.fs.rm(item.path, recurse=True)

remove_checkpoints(folder_path=checkpoint_bronze)
remove_checkpoints(folder_path=checkpoint_silver)
tablas = [Bronze_Orders, Silver_Orders, Gold_Inconsistencies_Report, Gold_Location_Customers, Gold_Location_Employees, Gold_Performance_Operations, Gold_Performance_Employees, Gold_Train_Model_Dataset, 'gold_demand_prediction', Bronze_Customers, Bronze_Employes, Bronze_Geodata, Bronze_Medellin, Silver_Customers, Silver_Employees]
for tabla in tablas:
    print(f"DELETE FROM `unalwater_v2`.`default`.`{tabla}`")
    spark.sql(f"DELETE FROM `unalwater_v2`.`default`.`{tabla}`")