import logging
import sys
import asyncio
import boto3
import os
import time
import datetime
import hashlib
import pickle
from botocore.exceptions import ClientError

ACCESS_KEY='AKIAZECOZT5B3A7F4PEZ'
SECRET_KEY='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'
 
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def mylog(fname, sum_str):
    f = open(fname, "a+")
    f.write(sum_str+"\n")
    f.close()
    if os.stat(fname).st_size > 5000000 : # logfile size > 5MB upload it to s3 in logbucket
        save_date= str(datetime.datetime.now())
        save_date=save_date.replace(' ' ,'')
        save_date=save_date.replace('-' ,'')
        save_date=save_date.replace(':' ,'')
        save_date=save_date.replace('.' ,'')
        fkey = save_date  + os.path.basename(fname)
        print('Uploading Log File')
        s3_client_log = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        s3_client_log.upload_file(fname,'mylogbucket0',fkey)
        print('Done, Log file saved to s3 mylogbucket0')
        os.rename(fname,os.path.dirname(fname)+"/"+ fkey)

    return()
def main():
    
    Base_command_1 = "convert -size 200x50 xc:none -matte -pointsize 20  -draw \"text 10,30 \'processed by EC2\'\" miff:- | composite -tile - "
    
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    sqs_client = boto3.client('sqs',region_name='us-east-2', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    buckets_list = s3_client.list_buckets()

    exists_resquest=False
    for bucket in buckets_list['Buckets']:
        if bucket['Name'] == 'requestsbucket':
            try :
                req_Q = sqs_client.get_queue_url(QueueName='requestQueue')
                req_Q_url = req_Q["QueueUrl"]
                exists_resquest=True
            except ClientError as e:
                logging.error(e)
                
    if exists_resquest :
        exists_response = False
        for bucket in buckets_list['Buckets']:
            if bucket['Name'] == 'responsesbucket':
                exists_response=True
                
        if not exists_response:   
            try:
                s3_client.create_bucket(ACL='private', Bucket='responsesbucket', CreateBucketConfiguration={'LocationConstraint': 'us-east-2'}, ObjectLockEnabledForBucket=True )
                s3_client.put_public_access_block( Bucket='responsesbucket', PublicAccessBlockConfiguration={'BlockPublicAcls': True,'IgnorePublicAcls': True,
                        'BlockPublicPolicy': True,'RestrictPublicBuckets': True } )
            except ClientError as e:
                logging.error(e)
        try :
            res_Q = sqs_client.get_queue_url(QueueName='responseQueue')
        except ClientError as e:
            logging.error(e)
            res_Q=sqs_client.create_queue(QueueName='responseQueue')
        res_Q_url = res_Q["QueueUrl"]

        print_wait_msg = True
        processed=[] # we keep keep track of processed files because of the delays of deleting from S3 and SQS, so we don't process the same file multiple times 
        while True :
            
            try:
                result = sqs_client.receive_message(QueueUrl=req_Q_url, AttributeNames=['SenderId'],
                                    MessageAttributeNames=['Request_DataType','Requested_Operations','File_Name','Input_Type',
                                                           'Output_Type','OutPut_File_Name','Time_Stamp','File_Md5_Hash'])
                if 'Messages' in result.keys() : # check for wainting messages
                    Message = result ['Messages'][0]
                    input_key = Message['Body']
                    input_type = Message['MessageAttributes']['Input_Type']['StringValue']
                    output_type = Message['MessageAttributes']['Output_Type']['StringValue']
                    tempfilename_input = './input/' +  input_key + '.' + input_type
                    tempfilename_output = './output/' +  input_key +  output_type
                    if input_key not in  processed:
                        print_wait_msg = True
                        s3_client.download_file('requestsbucket', input_key, tempfilename_input)
                        process_command_1 = Base_command_1 + "\"" +tempfilename_input + "\""+ " " + "\""+tempfilename_output+ "\""
                        os.system (process_command_1)
                        print("File Converted, Uploding it ...")
                        s3_client.upload_file(tempfilename_output,'responsesbucket',input_key)
                        print("Done")
                        SenderId = Message['Attributes']['SenderId']
                        MessageId = Message['MessageId']
                        file_name = Message['MessageAttributes']['File_Name']['StringValue']
                        file_type = Message['MessageAttributes']['Input_Type']['StringValue']
                        req_time_stamp  = Message['MessageAttributes']['Time_Stamp']['StringValue']
                        input_file_md5 = Message['MessageAttributes']['File_Md5_Hash']['StringValue']
                        output_type = Message['MessageAttributes']['Output_Type']['StringValue']
                        output_file_name = Message['MessageAttributes']['OutPut_File_Name']['StringValue']
                        time_stamp = str(datetime.datetime.now())
                        file_md5 = md5(tempfilename_output)
                        res_message = sqs_client.send_message(QueueUrl=res_Q_url, MessageBody=input_key,  MessageAttributes =
                                            {
                                                'Request_DataType': {
                                                    'StringValue':'AWS S3 File Key',
                                                    'DataType': 'String'
                                                    },
                                                'Requested_Operations' : {
                                                    'StringValue':'Convert',
                                                    'DataType': 'String'
                                                    },
                                                'File_Name' : {
                                                    'StringValue': file_name,
                                                    'DataType': 'String'
                                                    },
                                                'Input_Type' : {
                                                    'StringValue':file_type,
                                                    'DataType': 'String'
                                                    },
                                                'Output_Type' : {
                                                    'StringValue': output_type,
                                                    'DataType': 'String'
                                                    },
                                                'OutPut_File_Name' : {
                                                    'StringValue':output_file_name,
                                                    'DataType': 'String'
                                                    },
                                                'Time_Stamp' : {
                                                    'StringValue':time_stamp,
                                                    'DataType': 'String'
                                                    },
                                                'File_Md5_Hash' : {
                                                    'StringValue':file_md5,
                                                    'DataType': 'String'
                                                    },
                                                
                                                })
                        s3_client.delete_object(Bucket='requestsbucket', Key=input_key)
                        sqs_client.delete_message(QueueUrl=req_Q_url, ReceiptHandle=Message['ReceiptHandle'])
                        os.system ('rm -f '+ tempfilename_input) ## have issues here because of the delays, files are removed from server while still on the request/response queue
                        os.system ('rm -f '+ tempfilename_output) ## or the request/response buckets, FIFO queues could be a solution since a message in received multiple times in a queue, and handle S3 delays . files can be removed based on folder size trushold
                        processed.append(input_key)
                        summary = { 'operation_id:'+input_key : [{'SenderId' : SenderId}, {'MessageId' : MessageId}, {'Requested_Operations':'Image Convert'},{ 'File_Name' : file_name},{'Input_Type' : file_type}, {'Output_Type' : output_type},
                                                 {'OutPut_File_Name' : output_file_name}, {'Request_time_stamp': req_time_stamp} , {'Response_time_stamp': time_stamp },
                                                 {'Input_File_Md5_Hash':input_file_md5},{'Output_File_Md5_Hash':file_md5}
                                                 ]
                                    }
                        sum_str=str(summary)
                        print("operation sumary : " + sum_str +"\n" + "log saved to ./ConvertImagesLog.txt")
                        mylog('./ConvertImagesLog.txt', sum_str)
                else :
                    if print_wait_msg :
                        print('wainting for requests...')
                        print_wait_msg = False
                    time.sleep(2) # wait 2 sec before recheck
            except ClientError as e:
                logging.error(e)

if __name__ == "__main__":
    main()
