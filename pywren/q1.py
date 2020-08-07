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

def q1(key):
    #new s3 resource to work on
    s3 = boto3.resource(u's3')
    #read the file from s3 and split it on newline 
    lines=s3.Object("smile-tcp", key).get()['Body'].read().decode('utf-8').split("\n")
    print('all lines of file {} read'.format(key))

    #read the lines into a dicct format to work on
    rows= csv.DictReader(lines,delimiter='|', fieldnames=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])
    #print('all rows of file {} read'.format(key))
    #run the query for every line in the file
    q_date = date(1998,12,1) - timedelta(days=80)
    grouped={}
    for row in rows:
        parsed_date=row['L_SHIPDATE'].split("-")
        shipdate= date(int(parsed_date[0]),int(parsed_date[1]),int(parsed_date[2]))
        
        if  shipdate <= q_date:
            key = row['L_RETURNFLAG']+row['L_LINESTATUS']
            grouped.setdefault(key,[]).append([float(row['L_QUANTITY']), float(row['L_EXTENDEDPRICE']),float(row['L_DISCOUNT']),float(row['L_TAX'])])
    #filter None results for sum function
    
    #group the results by returnflag and linestatus
        
    results=[]
    #do all the math in the query
    for key in grouped:
        sum_qty=0
        sum_base_price=0
        sum_disc_price=0
        sum_charge=0
        sum_disc=0
        count=0

        for row in grouped[key]:
            sum_qty+=row[0]
            sum_base_price+=row[1]
            sum_disc_price +=row[1]*(1-row[2])
            sum_charge+=row[1]*(1-row[2])*(1+row[3])
            sum_disc+=row[2]
            count+=1
        results.append([key,sum_qty,sum_base_price,sum_disc_price,sum_charge,sum_disc,count])
    return results
#reduce the grouped results per file
def q1_reduce(raw):

    grouped={}
    for part in raw:
        for group in part:
            grouped.setdefault(group[0],[]).append(group[1:])
    results=[]
    for key in grouped:
        sum_qty=0
        sum_base_price=0
        sum_disc_price=0
        sum_charge=0
        sum_disc=0
        count=0
        for row in grouped[key]:
            sum_qty+=row[1]
            sum_base_price+=row[2]
            sum_disc_price +=row[3]
            sum_charge+=row[4]
            sum_disc+=row[5]
            count+=row[-1]
        results.append([key,sum_qty,sum_base_price,sum_disc_price,sum_charge,sum_qty/count,sum_base_price/count,sum_disc/count,count])
    return results
#run the query on pywren keys[0] is the Input folder so we ignore this one 
futures = wrenexec.map(q1, keys[1:])

#wait for futures to fill
raw=pywren.get_all_results(futures)
#reduce the results
res=q1_reduce(raw)
#format the output to fit the query
for key in sorted(res):
    print('group:{}, sum_qty:{:.2f}, sum_base_price:{:.2f}, sum_disc_price:{:.2f}, sum_charge:{:.2f}, avg_qty:{:.2f}, avg_price:{:.2f}, avg_disc:{:.2f}, count_order:{}'.format(key[0],key[1],key[2],key[3],key[4],key[5],key[6],key[7],key[8]))