import os, json, redis, pymysql, decimal
from dotenv import load_dotenv
from datetime import date, datetime
from multiprocessing import Pool, cpu_count

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
        return None

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
        return None
    
def get_da_codes():
    conn = connect_mariadb()
    if not conn:
        return []
    query = """
    SELECT dis.billing_date, dis.da_code
    FROM rdl_delivery_info_sap dis
    WHERE dis.billing_date = CURRENT_DATE
    GROUP BY dis.da_code;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

# ------------------ JSON Serializer ------------------ #
def custom_serializer(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj

# ------------------ Process Caching ------------------ #
def process_cache(chunk):
    conn = connect_mariadb()
    r = connect_redis()
    if not conn or not r:
        print(f"Failed to connect to MariaDB or Redis at {datetime.now()}")
        return

    cursor = conn.cursor()
    for billing_date, da_code in chunk:
        cache_key = f"{billing_date}_{da_code}_delivery-info"
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
                AND dis.da_code = %s;
        """
        try:
            cursor.execute(query, (billing_date, da_code))
            data = cursor.fetchall()
        except Exception as e:
            print(f"Error fetching {da_code} - {billing_date} : {e} at {datetime.now()}")
            continue

        if not data:
            print(f"No data found for {da_code} - {billing_date} - {datetime.now()}")
            continue

        # Convert the result to a list of dictionaries
        column_names = [desc[0] for desc in cursor.description]
        data_dict = [dict(zip(column_names, row)) for row in data]
        json_data = json.dumps(data_dict, default=custom_serializer)

        r.set(cache_key, json_data)
        print(f"{cache_key} saved - {datetime.now()}")

    cursor.close()
    conn.close()

# ------------------ Helper: Split into Chunks ------------------ #
def split_chunks(da_codes, n):
    k, m = divmod(len(da_codes), n)
    return [da_codes[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]

# Run multiprocessing
if __name__ == "__main__":
    print(f"Process started at {datetime.now()}")
    start_time = datetime.now()
    
    da_codes = get_da_codes()
    print(f"Total DA: {len(da_codes)} - {datetime.now()}")
    if not da_codes:
        print(f"No DA codes found at {datetime.now()}.")
        exit()

    num_processes = min(15, len(da_codes))
    chunks = split_chunks(da_codes, num_processes)
    
    with Pool(num_processes) as pool:
        pool.map(process_cache,  chunks)
        
    print(f"Process completed at {datetime.now()}")
    end_time = datetime.now()
    print(f"Total time taken: {end_time - start_time} seconds")