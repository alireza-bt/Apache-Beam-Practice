"""OCI Bucket Module
This module helps with bucket-related actions on Oracle Cloud.
Supported actions:
- list_buckets
- list_bucket_counts
- list_bucket_details
- check_bucket_exists
- create_bucket
- delete_bucket
- upload_file
- upload_directory
- download_bucket
- download_bucket_file

"""

import oci
import json
import os
import io

#load the config file
# config = oci.config.from_file("~/.oci/config")
config = oci.config.from_file()

#My Compartment ID
COMPARTMENT_ID = config['compartment_id']

#Object storage Namespace
object_storage_client = oci.object_storage.ObjectStorageClient(config)
NAMESPACE = object_storage_client.get_namespace().data


def list_buckets():
    l_buckets = object_storage_client.list_buckets(NAMESPACE, COMPARTMENT_ID).data
    # Get the data from response
    for bucket in l_buckets:
        print(bucket.name)

def list_bucket_counts():
    l_buckets = object_storage_client.list_buckets(NAMESPACE, COMPARTMENT_ID).data
    for bucket in l_buckets:
        print("Bucket name: ",bucket.name)
        buck_name = bucket.name
        objects = object_storage_client.list_objects(NAMESPACE, buck_name).data
        count = 0
        for i in objects.objects:
            count+=1
    
        print('... num of objects :', count)

def check_bucket_exists(b_name):
    #check if Bucket exists
    is_there = False
    l_b = object_storage_client.list_buckets(NAMESPACE, COMPARTMENT_ID).data
    for bucket in l_b:
        if bucket.name == b_name:
            is_there = True
        
    if is_there == True:
        print(f'Bucket {b_name} exists.')
    else:
        print(f'Bucket {b_name} does not exist.')
        
    return is_there

def list_bucket_details(b):
    bucket_exists = check_bucket_exists(b)
    if bucket_exists == True:
        objects = object_storage_client.list_objects(NAMESPACE, b).data
        print(f'Bucket {b} has objects :', len(objects.objects))

def create_bucket(b):
    #create Bucket if it does not exist
    bucket_exists = check_bucket_exists(b)
    if bucket_exists == False:
        try:
            create_bucket_response = object_storage_client.create_bucket(
                NAMESPACE,
                oci.object_storage.models.CreateBucketDetails(
                    name=b,
                    compartment_id=COMPARTMENT_ID
                )
            )
            bucket_exists = True
            # Get the data from response
            print(f'Created Bucket {create_bucket_response.data.name}')
        except Exception as e:
            print(e.message)
    else:
        bucket_exists = True
        print(f'... nothing to create.')
        
    return bucket_exists

def delete_bucket(b_name):
    bucket_exists = check_bucket_exists(b_name)
    objects_exist = False
    if bucket_exists == True:
        print('Starting - Deleting Bucket '+b_name)
        print('... checking if objects exist in Bucket (bucket needs to be empty)')
        try:
            object_list = object_storage_client.list_objects(NAMESPACE, b_name).data
            objects_exist = True
        except Exception as e:
            objects_exist = False

        if objects_exist == True:
            print('... ... Objects exists in Bucket. Deleting these objects.')
            count = 0
            for o in object_list.objects:
                count+=1
                object_storage_client.delete_object(NAMESPACE, b_name, o.name)
    
            if count > 0:
                print(f'... ... Deleted {count} objects in {b_name}')
            else:
                print(f'... ... Bucket is empty. No objects to delete.')
                
        else:
            print(f'... No objects to delete, Bucket {b_name} is empty')

            
        print(f'... Deleting bucket {b_name}')    
        response = object_storage_client.delete_bucket(NAMESPACE, b_name)
        print(f'Deleted bucket {b_name}')  


def upload_file(b, f, bucket_file_name):
    file_exists = os.path.isfile(f)
    if file_exists == True:        
        #check to see if Bucket exists
        b_exists = check_bucket_exists(b)
        if b_exists == True:
            print(f'... uploading {f}')
            try:
                # object_storage_client.put_object(NAMESPACE, b, os.path.basename(f), io.open(f,'rb'))
                object_storage_client.put_object(NAMESPACE, b, bucket_file_name, io.open(f,'rb'))
                print(f'. finished uploading {f}')
            except Exception as e:
                print(f'Error uploading file. Try again.')
                print(e)
        else:
            print('... Create Bucket before uploading file.')

    else:
        print(f'... File {f} does not exist or cannot be found. Check file name and full path')


def upload_directory(b, d):
    count = 0
    
    #check to see if Bucket exists
    b_exists = check_bucket_exists(b)
    if b_exists == True:
        #loop files
        for filename in os.listdir(d):
            print(f'... uploading {filename}')
            try:
                object_storage_client.put_object(NAMESPACE, b, filename, io.open(os.path.join(d,filename),'rb'))
                count += 1
            except Exception as e:
                print(f'... ... Error uploading file. Try again.')
                print(e)

    else:
        print('... Create Bucket before uploading files.')
        
    if count == 0:
        print('... Empty directory. No files uploaded.')
    else:
        print(f'Finished uploading Directory : {count} files into {b} bucket')

def download_bucket(b, d):
    if os.path.exists(d) == True:
        print(f'{d} already exists.')
    else:
        print(f'Creating {d}')
        os.makedirs(d)
        
    print('Downloading Bucket:',b)
    object_list = object_storage_client.list_objects(NAMESPACE, b).data
    count = 0
    for i in object_list.objects:
        count+=1
            
    print(f'... {count} files')
    
    for o in object_list.objects:
        print(f'Downloading object {o.name}')
        get_obj = object_storage_client.get_object(NAMESPACE, b, o.name)
        with open(os.path.join(d,o.name), 'wb') as f:
            for chunk in get_obj.data.raw.stream(1024 * 1024, decode_content=False):
                f.write(chunk)
    
    print('Download Finished.')

def download_bucket_file(b, d, f):
    print('Downloading File:',f, ' from Bucket', b)
    
    try:
        get_obj = object_storage_client.get_object(NAMESPACE, b, f)
        with open(os.path.join(d, f), 'wb') as f:
            for chunk in get_obj.data.raw.stream(1024 * 1024, decode_content=False):
                f.write(chunk)
        print('Download Finished.')
    except:
        print('Error trying to download file. Check parameters and try again')