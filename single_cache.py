import redis 
import pymysql
import json
import decimal
import sys
from datetime import date 

# Connect to Redis
try:
    redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    r = redis.Redis(connection_pool=redis_pool)
except :
    print(f"Failed to connect to Redis")

# Connect to MariaDB
conn = pymysql.connect(
    host='160.191.150.60',
    user='root',
    password='5w5A0V&eWP',
    database='odms_db' 
)
cursor = conn.cursor()

# Query for delivery info sap
da_code = sys.argv[1]
billing_date = sys.argv[2]
if billing_date == "1":
    billing_date = date.today()

print(billing_date)

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
        LEFT JOIN rdl_delivery_list dl ON sis.matnr = dl.matnr AND d.id = dl.delivery_id
    WHERE
        dis.billing_date = %s 
        AND dis.da_code = %s ;
    """
cursor.execute(query,(billing_date, da_code))
data=cursor.fetchall() 
    
# Convert the result to a list of dictionaries
column_names = [desc[0] for desc in cursor.description]
data_dict = [dict(zip(column_names, row)) for row in data]
    
# Generate key
cache_key = f"{billing_date}_{da_code}_delivery-info"

# Custom Serializer
def custom_serializer(obj):
    if isinstance(obj, date):
        return obj.isoformat()  # Convert date to string (YYYY-MM-DD format)
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj

# Convert data into json
json_data=json.dumps(data_dict,default=custom_serializer)

# Save the data to Redis as a JSON string
r.set(cache_key,json_data) 
print(f"{cache_key} saved")

# Close the cursor and connection
cursor.close()
conn.close()