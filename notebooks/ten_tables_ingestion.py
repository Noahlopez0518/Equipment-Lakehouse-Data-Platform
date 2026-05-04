# ================================
# IMPORTS + CONFIG
# ================================
import requests, json, pandas as pd, time
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, to_json, base64, max as spark_max
from pyspark.sql.types import *

API_TOKEN, BASE_URL = "", "https://api.tenna.com/v1"
LAKEHOUSE, LIMIT, RETRY_DELAY, MAX_RETRIES = "Tenna_Data.Tenna_Landing.dbo", 100, 60, 5
BUFFER_MIN = 10
spark.sql(f"USE {LAKEHOUSE}")

# ================================
# WATERMARKS
# ================================
wm_table = f"{LAKEHOUSE}.watermarks"
wm_schema = StructType([
    StructField("endpoint", StringType(), False),
    StructField("last_updated_at", TimestampType(), True),
    StructField("saved_at", TimestampType(), False)
])

if not spark.catalog.tableExists(wm_table):
    spark.createDataFrame([], wm_schema).write.format("delta").saveAsTable(wm_table)

get_wm = lambda ep: (
    (lambda r: r[0][0] - timedelta(minutes=BUFFER_MIN) if r and r[0][0] else None)
    (spark.sql(f"SELECT last_updated_at FROM {wm_table} WHERE endpoint='{ep}' ORDER BY saved_at DESC LIMIT 1").collect())
)

def save_wm(ep, df, colname):
    if colname in df.columns:
        ts = df.agg(spark_max(col(colname))).collect()[0][0]
        if ts:
            spark.createDataFrame([(ep, ts, datetime.now())], wm_schema)\
                 .write.format("delta").mode("append").saveAsTable(wm_table)

# ================================
# API
# ================================
def api_get(url, headers, params):
    for i in range(MAX_RETRIES):
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code in [429,430]:
            time.sleep(RETRY_DELAY*(i+1)); continue
        r.raise_for_status(); return r
    raise Exception("Max retries")

def fetch(ep, params={}):
    url, h, recs, off = f"{BASE_URL}/{ep}", {"Authorization":f"Bearer {API_TOKEN}"}, [], 0
    while True:
        p = {"limit":LIMIT,"offset":off}|params
        page = api_get(url,h,p).json().get("results",[])
        recs += page
        if len(page)<LIMIT: break
        off += LIMIT; time.sleep(.3)
    return recs

# ================================
# TRANSFORM
# ================================
def clean(pdf):
    for c in pdf.columns:
        if pdf[c].apply(lambda x:isinstance(x,(list,dict))).any():
            pdf[c]=pdf[c].apply(lambda x:json.dumps(x) if isinstance(x,(list,dict)) else x)
    return pdf

def cast(df, dt=[], dbl=[], lng=[], boo=[]):
    for c,t in [(dt,TimestampType),(dbl,DoubleType),(lng,LongType),(boo,BooleanType)]:
        for coln in c:
            if coln in df.columns: df=df.withColumn(coln,col(coln).cast(t()))
    for f in df.schema.fields:
        n,t=f.name,f.dataType
        if isinstance(t,(StructType,MapType,ArrayType)): df=df.withColumn(n,to_json(col(n)))
        elif isinstance(t,BinaryType): df=df.withColumn(n,base64(col(n)).cast(StringType()))
        elif isinstance(t,DecimalType): df=df.withColumn(n,col(n).cast(DoubleType()))
        elif isinstance(t,NullType): df=df.withColumn(n,lit(None).cast(StringType()))
    return df

# ================================
# WRITE
# ================================
def write(df, table, pk, cpk=None):
    name=f"{LAKEHOUSE}.{table}"
    if not spark.catalog.tableExists(name):
        df.write.format("delta").mode("overwrite").saveAsTable(name)
    else:
        keys=cpk or [pk]
        cond=" AND ".join([f"target.{k}=source.{k}" for k in keys])
        df.dropDuplicates(keys).createOrReplaceTempView("_src")
        spark.sql(f"MERGE INTO {name} target USING _src source ON {cond} "
                  f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

# ================================
# ENGINE
# ================================
def run(cfg):
    ep, tbl, pk = cfg["endpoint"], cfg["table"], cfg["pk"]
    dt, dbl, lng, boo = cfg.get("dt",[]), cfg.get("dbl",[]), cfg.get("lng",[]), cfg.get("boo",[])
    upd, fp, full, cpk = cfg.get("upd","updated_at"), cfg.get("fp","updated_from"), cfg.get("full",False), cfg.get("cpk")

    print(f"\n{ep} → {tbl}")
    params={}
    if not full and spark.catalog.tableExists(f"{LAKEHOUSE}.{tbl}"):
        wm=get_wm(ep)
        params[fp]=(wm or datetime.now()-timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

    recs=fetch(ep,params)
    if not recs: return

    df=spark.createDataFrame(clean(pd.DataFrame(recs)))
    df=cast(df,dt,dbl,lng,boo)

    if full or not spark.catalog.tableExists(f"{LAKEHOUSE}.{tbl}"):
        df.write.format("delta").mode("overwrite").saveAsTable(f"{LAKEHOUSE}.{tbl}")
    else:
        write(df,tbl,pk,cpk)

    save_wm(ep,df,upd)

# ================================
# CONFIG (ADD ALL TABLES HERE)
# ================================
CONFIG=[
    {"endpoint":"assets","table":"assets","pk":"asset_id","dt":["created_at","updated_at"],"full":True},
    {"endpoint":"asset-financials","table":"asset_financials","pk":"asset_id","dt":["purchase_date","updated_at"],"full":True},
    {"endpoint":"asset-assignee-history","table":"asset_assignee_history","pk":"asset_assignee_history_id","dt":["assignment_start","updated_at"]},
    {"endpoint":"asset-utilizations-daily","table":"asset_utilizations_daily","pk":"asset_id","dt":["date","updated_at"],"cpk":["asset_id","date"],"fp":"date_from"}
]

# ================================
# RUN
# ================================
print(f"START: {datetime.now()}")
for c in CONFIG: run(c)
print(f"END: {datetime.now()}")
