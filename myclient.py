import logging
import boto3
import os
import datetime
import hashlib
import imghdr
import time
from botocore.exceptions import ClientError

ACCESS_KEY='AKIAZECOZT5B3A7F4PEZ'
SECRET_KEY=''
client_id='cps2student' # just to treat the case of different clients, it could be managed more rigorously with appropriate libs (connected or connectionless). 

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def main():
    
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    sqs_client = boto3.client('sqs', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)  

    buckets_list = s3_client.list_buckets()
    
    exists = False
    for bucket in buckets_list['Buckets']:
        if bucket['Name'] == 'requestsbucket':
            exists=True
    if not(exists):
        try:
            s3_client.create_bucket(ACL='private', Bucket='requestsbucket', CreateBucketConfiguration={'LocationConstraint': 'us-east-2'}, ObjectLockEnabledForBucket=True )
            s3_client.put_public_access_block( Bucket='requestsbucket', PublicAccessBlockConfiguration={'BlockPublicAcls': True,'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,'RestrictPublicBuckets': True } )
           # print('requestsbucket created')
        except ClientError as e:
            logging.error(e)
    try :
        req_Q = sqs_client.get_queue_url(QueueName='requestQueue')
    except ClientError as e:
        logging.error(e)
        req_Q=sqs_client.create_queue(QueueName='requestQueue')

    req_Q_url = req_Q["QueueUrl"]

    Input = input('Converter: >>>  ')
    Inputs = Input.split()

    wrong_input=False

    if len(Inputs)!=2 :
        wrong_input=True
    elif not os.path.isfile(Inputs[0]):
        wrong_input=True
        
    while wrong_input :
        while len(Inputs)!=2 :
            print('Wrong argument number try again.\n')
            Input = input('Converter: >>>  ')
            Inputs = Input.split()
        if os.path.isfile(Inputs[0]) :
            wrong_input=False
        else :
            print('No such file, insert path to file. try again\n')
            Input = input('Converter: >>>  ')
            Inputs = Input.split()
    

    intput_file_name=Inputs[0]
    output_file_name=Inputs[1]
    output_type=os.path.splitext(output_file_name)[1]
    time_stamp = str(datetime.datetime.now())
    file_md5=md5(intput_file_name)
    file_name = os.path.basename(intput_file_name)
    file_type = imghdr.what(intput_file_name)
    file_meta = time_stamp + file_name + client_id
    
    file_key = (hashlib.md5(file_meta.encode())).hexdigest() + file_md5

    try :
        print('Uploading file ...')
        s3_client.upload_file(intput_file_name,'requestsbucket',file_key)
        print('Done.')
        req_message = sqs_client.send_message(QueueUrl=req_Q_url, MessageBody=file_key,  MessageAttributes =
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
        time.sleep(2) # waiting for server to process
        attemps = 0
        exists_response = False
        done = False
        print_wait_msg = True
        while attemps < 30 and not done :
            if not exists_response :
                for bucket in buckets_list['Buckets']:
                    if bucket['Name'] == 'responsesbucket':
                            exists_response=True

            res_Q = sqs_client.get_queue_url(QueueName='responseQueue')
            res_Q_url = res_Q["QueueUrl"]
                       
            if exists_response :
                result = sqs_client.receive_message(QueueUrl=res_Q_url, AttributeNames=['SenderId'],
                                    MessageAttributeNames=['Request_DataType','Requested_Operations','File_Name','Input_Type',
                                                           'Output_Type','OutPut_File_Name','Time_Stamp','File_Md5_Hash'])
                if 'Messages' in result.keys()  : # check for wainting messages
                    print("there is messages in response queue")
                    Message = result ['Messages'][0]
                    print(Message['Body'])
                    if  Message['Body'] == file_key :
                        print('Downloading file ...')
                        s3_client.download_file('responsesbucket', file_key , output_file_name)
                        s3_client.delete_object(Bucket='responsesbucket', Key = file_key)
                        sqs_client.delete_message(QueueUrl=res_Q_url, ReceiptHandle=Message['ReceiptHandle'])
                        print('Done.')
                        done = True
                    else :
                        print("not the file we uploaded")
                elif  print_wait_msg :
                    print('No messages in Queue yet, wainting ...')
                    print_wait_msg = False 
                time.sleep(2)
                attemps+=1
                #print('another attemp')
        if done:
            sucess_msg='Success ! \n'+ output_file_name +' is ready'
            print(sucess_msg)
        else :
            print('Fail, request timed out.')
    except ClientError as e:
        logging.error(e)

if __name__ == "__main__":
        print(    """
 Online convertion utility using AWS.
 usage :  InputFilePath  OutputFilePath
    """)
        main()

    
