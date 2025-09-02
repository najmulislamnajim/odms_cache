import os, sys, json, redis, pymysql, decimal
from dotenv import load_dotenv
from datetime import date, datetime

# ------------------ Environment ------------------ #
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")

# ------------------ Connections ------------------ #
def connect_redis():
    try:
        redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=redis_pool)
        return r
    except Exception as e:
        print(f"Redis connection failed: {e} at {datetime.now()}")
        exit()

def connect_mariadb():
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connect_timeout=600,
            read_timeout=600,
            write_timeout=600,
            autocommit=True
        )
        return conn
    except Exception as e:
        print(f"MariaDB connection failed: {e} at {datetime.now()}")
        exit()

# ------------------ JSON Serializer ------------------ #
def custom_serializer(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj

# --------------- Query for delivery info sap --------------
print(f"Process started at {datetime.now()}")
start_time = datetime.now()

da_code = sys.argv[1]
billing_date = sys.argv[2]
if billing_date == "1":
    billing_date = date.today()

query = """
    SELECT
        dis.billing_doc_no,
        dis.billing_date,
        dis.route,
        dis.da_code,
        dis.da_name,
        dis.vehicle_no,
        sis.gate_pass_no,
        sis.partner,
        sis.matnr,
        sis.batch,
        sis.quantity,
        sis.net_val,
        sis.tp,
        sis.vat,
        sis.billing_type,
        sis.assigment,
        sis.plant,
        sis.team,
        sis.created_on,
        r.route_name,
        CONCAT(c.name1, ' ', c.name2) AS customer_name,
        c.contact_person AS partner_name,
        c.mobile_no AS customer_mobile,
        CONCAT(
            c.street, ', ',
            c.street1, ', ',
            c.street2, ', ',
            c.upazilla, ', ',
            c.district
        ) AS customer_address,
        cl.latitude AS customer_latitude,
        cl.longitude AS customer_longitude,
        cl.latitude,
        cl.longitude,
        m.material_name,
        m.producer_company,
        m.brand_name,
        m.brand_description,
        (
            SELECT SUM(d2.due_amount)
            FROM rdl_delivery d2
            WHERE d2.partner = sis.partner
            AND d2.billing_date < CURRENT_DATE
        ) AS previous_due_amount,
        d.id,
        IF(d.delivery_status IS NULL, 'Pending', d.delivery_status) AS delivery_status,
        IF(d.cash_collection_status IS NULL, 'Pending', d.cash_collection_status) AS cash_collection_status,
        d.return_status,
        d.net_val AS delivered_amount,
        d.cash_collection,
        d.return_amount,
        d.transport_type,
        dl.id AS list_id,
        dl.delivery_quantity,
        dl.return_quantity,
        dl.return_net_val,
        dl.delivery_net_val
    FROM
        rdl_delivery_info_sap dis
        INNER JOIN rpl_sales_info_sap sis ON dis.billing_doc_no = sis.billing_doc_no
        LEFT JOIN rdl_route_wise_depot r ON dis.route = r.route_code
        INNER JOIN rpl_customer c ON sis.partner = c.partner
        LEFT JOIN rdl_customer_location cl ON sis.partner = cl.customer_id
        LEFT JOIN rpl_material m ON sis.matnr = m.matnr
        LEFT JOIN rdl_delivery d ON dis.billing_doc_no = d.billing_doc_no
        LEFT JOIN rdl_delivery_list dl ON sis.matnr = dl.matnr AND sis.batch = dl.batch AND d.id = dl.delivery_id
    WHERE
        dis.billing_date = %s 
        AND dis.da_code = %s ;
    """

# Connect to MariaDB
conn = connect_mariadb()
cursor = conn.cursor()
try:
    cursor.execute(query,(billing_date, da_code))
    data=cursor.fetchall() 
except Exception as e:
    print(f"Error fetching data for {da_code}: {e} at {datetime.now()}")
    cursor.close()
    conn.close()
    exit()
    
column_names = [desc[0] for desc in cursor.description]
data_dict = [dict(zip(column_names, row)) for row in data]
json_data=json.dumps(data_dict,default=custom_serializer)

cache_key = f"{billing_date}_{da_code}_delivery-info"
r = connect_redis()
r.set(cache_key,json_data) 
print(f"{cache_key} saved - {datetime.now()}")
cursor.close()
conn.close()

print(f"Process completed at {datetime.now()}")
end_time = datetime.now()
print(f"Total time taken: {end_time - start_time} seconds")