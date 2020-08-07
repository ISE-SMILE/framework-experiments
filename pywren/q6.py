import pywren
import csv
from datetime import date,timedelta
import boto3



# get a handle on s3
s3 = boto3.resource(u's3')

# get a handle on the bucket that holds your file
bucket = s3.Bucket(u'smile-tcp')

# get a handle on the object in the Input Folder
prefix_objs = bucket.objects.filter(Prefix="Input")

# get the keys of the files we want to read
keys=[]
for obj in prefix_objs:
    keys.append(obj.key)

#get a pywren executor
wrenexec = pywren.default_executor()

#define the query
def q6(key):
    #new s3 resource to work on
    s3 = boto3.resource(u's3')
    #read the file from s3 and split it on newline 
    lines=s3.Object("smile-tcp", key).get()['Body'].read().decode('utf-8').split("\n")
    #read the lines into a dicct format to work on
    rows= csv.DictReader(lines,delimiter='|', fieldnames=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])
    results=[]
    #run the query for every line in the file
    for row in rows:
        parsed_date=row['L_SHIPDATE'].split("-")
        shipdate= date(int(parsed_date[0]),int(parsed_date[1]),int(parsed_date[2]))
        q_date = date(1994,1,1)
        year=timedelta(days=365)
        if int(row['L_QUANTITY']) < 24 and float(row['L_DISCOUNT'])<0.08 and float(row['L_DISCOUNT'])>0.06 and shipdate >= q_date and shipdate < (q_date+year):
            results.append( float(row['L_EXTENDEDPRICE'])*float(row['L_DISCOUNT']))
    #filter None results for sum function
    results = list(filter(None, results))
    
    return sum(results)
#run the query on pywren keys[0] is the Input folder so we ignore this one 
futures = wrenexec.map(q6, keys[1:])
#wait for futures to fill
raw=pywren.get_all_results(futures)
#final result
print(sum(raw))
    


