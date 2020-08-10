import json
import os
import sys
import csv
import re
import boto3
import botocore
import gzip
import time
##from retrying import retry
import yaml

#s3_conf_bucket = os.environ['CONFIG_BUCKET']
region = os.environ['REGION']
#s3_db_bucket = os.environ['SOURCE_BUCKET']
#s3_db_path = os.environ['SOURCE_PATH']
#s3_bucket = os.environ['OUT_BUCKET']  ##'dev-driscolls-datalake-config'
#env = os.environ['SOURCE']

# init clients

athena = boto3.client('athena')
s3r = boto3.resource('s3')
s3 = boto3.client('s3')
glu = boto3.client('glue')


def dtype_con(d_types):
    if d_types == 'int' or d_types == 'bigint' or d_types == 'tinyint':
        return 'bigint'
    elif d_types == 'varchar' or d_types == 'nvarchar' or d_types == 'text' or d_types == 'char':
        return 'string'
    elif d_types == 'datetime' or d_types == 'timestamp':
        return 'timestamp'
        ##string'
        ##timestamp'
    elif d_types == 'date':
        return 'date'
    elif d_types == 'float' or d_types == 'decimal':
        return 'float'
    elif d_types == 'bit':
        ##return 'string'
        return 'boolean'
    elif d_types == 'double':
        return 'double'
        #### bigint
    elif re.search('double.', d_types):
        return d_types.replace('double', 'decimal')
    elif d_types == 'numeric':
        return 'double'
        ####'bigint'
    else:
        return 'string'


def poll_status(_id):
    result = athena.get_query_execution(QueryExecutionId=_id)

    state = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        return result


def run_query(query, database, s3_output):
    response = athena.start_query_execution(

        QueryString=query,

        QueryExecutionContext={

            'Database': database

        },
        ResultConfiguration={

            'OutputLocation': s3_output,
        })
    print("Output: ", response)

    QueryExecutionId = response['QueryExecutionId']
    time.sleep(2)
    print('Wait for 2 secs for query to get executed then check the result')
    result = poll_status(QueryExecutionId)

    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        print("Query SUCCEEDED: {}".format(QueryExecutionId))
        return 'SUCCEEDED'
    elif result['QueryExecution']['Status']['State'] == 'RUNNING':
        while True:
            result = poll_status(QueryExecutionId)
            if result['QueryExecution']['Status']['State'] == 'RUNNING':
                print('Going for a sleep')
                time.sleep(1)
            else:
                break
        print('Breaking out of loop as query status has changed')
        return result['QueryExecution']['Status']['State']
    else:
        return result['QueryExecution']['Status']['State']


def lambda_handler(event, context):
    database = event['database_transformed']
    env = event['properties_source']
    s3_db_path = event['source_orc']
    # s3_db_bucket = event['db_bucket']
    runtime = event['env']

    ##if runtime == 'test':
    s3_conf_bucket = event['config_bucket']
    config_file = event['config_file']
    print('Config Bucket {}'.format(s3_conf_bucket))
    config_yml_obj = s3.get_object(Bucket=s3_conf_bucket, Key=config_file)
    config_yml_body = config_yml_obj['Body']
    cym = yaml.load(config_yml_body)
    ckeys = [k for k in cym[runtime]]
    #s3_db_bucket = cym[runtime]['sensitive_bucket']
    s3_db_bucket = event['source_bucket']
    #properties_file = cym[runtime]['properties_file']
    s3_db_bucket = event['source_bucket']
    properties_file = event['properties_file']
    s3_bucket = cym[runtime]['athena_bucket']
    print(database, s3_db_path, s3_db_bucket, properties_file, runtime)
    res_ls = s3.list_objects_v2(Bucket=s3_db_bucket, Prefix=s3_db_path, Delimiter='/')
    dir_set = [x['Prefix'].split('/')[-2] for x in res_ls['CommonPrefixes']]
    low_dir_set = [str.lower(i) for i in dir_set]
    print('directory to scan: {}'.format(dir_set))
    properties_yml_obj = s3.get_object(Bucket=s3_conf_bucket, Key=properties_file)
    properties_yml_body = properties_yml_obj['Body']
    dym = yaml.load(properties_yml_body)
    '''config_yml_obj = s3.get_object(Bucket=s3_conf_bucket, Key=config_file)
    config_yml_body = config_yml_obj['Body']
    cym = yaml.load(config_yml_body)'''
    pkeys = [k for k in dym[env]['tables']]
    spkeys = [str.lower(i) for i in pkeys]
    print('Table Names picked from properties file:  and YML keys: {}{}'.format(pkeys, spkeys))
    #####Check the existnce of the database#######
    db_loc = "'s3://" + s3_db_bucket + "/" + s3_db_path + "'"
    print('DB Location :{}'.format(db_loc))
    s3_output = 's3://' + s3_bucket + '/test_output'
    glu_res = glu.get_databases()
    elist = []
    for d in glu_res['DatabaseList']:
        if 'Name' in d:
            elist.append(d['Name'])
    print('response from Glue for available DataBases: {}'.format(elist))
    if database not in elist:
        print('Database is not available, Create the database: {}'.format(database))
        create_db_qry = ("""create database {} location {}""".format(database, db_loc))
        result_set = run_query(create_db_qry, database, s3_output)
        print('Database creation status: {}'.format(result_set))
    else:
        print('Database already exists')
        ##rows = obj1.get()['Body'].read().split('\n').decode('utf-8')
    ##rows = obj1['Body'].read().split('\n').decode('utf-8')
    ##for tab_name in dir_set:
    for tabl_name in spkeys:
        table_name = pkeys[spkeys.index(tabl_name)]
        # print('Table name picked from landing directory sub folder : {}'.format(unicode.lower(tab_name)))
        # tabl_name=unicode.lower(tab_name)
        for kk in dym[env]["tables"][table_name]:
            if (kk == 'enable' and dym[env]["tables"][table_name]['enable'] == ['True']):
                print(dym[env]["tables"][table_name])

                ##print(dym["qa"]["tables"][table_name]['enable'])
                if tabl_name in low_dir_set:  ##for k in pkeys:
                    ##For c,lc in zip(pkeys,spkeys):
                    print("Table name present from property file in the list")

                    ##aq_st = "".join(dym["qa"]["tables"][table_name]['primary_key'])
                    it2 = iter([dtype_con(li) for li in dym[env]["tables"][table_name]['data_types']])
                    it1 = iter(dym[env]["tables"][table_name]['columns'])

                    '''q_st = '(' '''
                    cqst = []
                    if 'partitions' in dym[env]["tables"][table_name]:
                        ###len(dym["qa"]["tables"][table_name]['partition'])>0:
                        print('Going for a partitioned table')
                        [cqst.append(i + ' ' + j) for i, j in zip(it1, it2)]
                        print(','.join(cqst))
                        '''cqst.remove(','.join(dym["qa"]["tables"][table_name]['partition']))
                        print(','.join(cqst))'''
                        loc = "'s3://" + s3_db_bucket + "/" + s3_db_path + tabl_name + "/'"
                        drop_tbl_query = ("""DROP TABLE IF EXISTS {}""".format(tabl_name))
                        result_set = run_query(drop_tbl_query, database, s3_output)
                        print(result_set)
                        ##query = ("""create external table IF NOT EXISTS {}({}) PARTITIONED BY ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION {} """.format(table_name,','.join(cqst),','.join(dym["qa"]["tables"][table_name]['partition']),loc))
                        query = (
                            """create external table IF NOT EXISTS {}({}) PARTITIONED BY (dt date) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION {} tblproperties ("skip.header.line.count"="1")""".format(
                                tabl_name, ','.join(cqst), loc))
                        print(query)
                        query1 = query.replace('*', ',')
                        print(query1)
                        result = run_query(query1, database, s3_output)
                        print("Results:{}".format(result))
                        ##print(result)
                        if result == 'SUCCEEDED':
                            file_name = []
                            my_bucket = s3r.Bucket(s3_db_bucket)
                            file_loc = s3_db_path + tabl_name + "/"  ###Lower case letter in the directory
                            tbl_loc = "'s3://" + s3_db_bucket + "/" + s3_db_path
                            print('File location: {}'.format(file_loc))
                            for b in my_bucket.objects.filter(Prefix=file_loc):
                                file_name.append(b.key.split('/')[-2])
                            file_name = list(set(file_name))
                            f_name = ["""PARTITION (dt = '{}') location {}{}/{}/'""".format(i.split('=')[1], tbl_loc,
                                                                                            tabl_name, i) for i in
                                      file_name]
                            print('Partitions: {}'.format(f_name))
                            alt_stmt = 'ALTER TABLE {} ADD IF NOT EXISTS {}'.format(tabl_name, ' '.join(f_name))
                            print('Alter Statement:  {}'.format(alt_stmt))
                            alter_result = run_query(alt_stmt, database, s3_output)
                            print("Alter Results:")
                            print(alter_result)
                        else:
                            print('Unable to create a partitioned table: {}'.format(tabl_name))
                    else:
                        print('going for non partitioned table')
                        [cqst.append(i + ' ' + j) for i, j in zip(it1, it2)]
                        print(','.join(cqst))
                        loc = "'s3://" + s3_db_bucket + "/" + s3_db_path + tabl_name + "/'"
                        print('Location :{}'.format(loc))
                        drop_tbl_query = ("""DROP TABLE IF EXISTS {}""".format(tabl_name))
                        result_set = run_query(drop_tbl_query, database, s3_output)
                        query = (
                            """create external table IF NOT EXISTS {}({}) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION {} tblproperties ("skip.header.line.count"="1")""".format(
                                tabl_name, ','.join(cqst), loc))
                        print(query)
                        query1 = query.replace('*', ',')
                        result = run_query(query1, database, s3_output)
                        print("Results:")
                else:
                    print('Not matching with object..')