import boto3
import yaml
import json
from botocore.exceptions import NoCredentialsError, ClientError

class S3():
        
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = None
        self.s3 = None
        self._load_config()
        self.bucket_name=self.config['bucket_name']

    def _load_config(self):
        try:
            with open(self.config_path, 'r') as config_file:
                self.config = yaml.safe_load(config_file)
        
            if self.config:
                self.s3 = boto3.client(
                    's3',
                    aws_access_key_id=self.config['aws_access_key_id'],
                    aws_secret_access_key=self.config['aws_secret_access_key'],
                    region_name=self.config['region_name'],
                    endpoint_url=self.config['endpoint_url']
                )
        except FileNotFoundError:
            print(f"Error: Configuration file '{self.config_path}' not found")
        except yaml.YAMLError as e:
            print(f"Error parsing YAML in configuration file: {e}")
        except KeyError as e:
            print(f"Error: Missing key '{e}' in configuration file.")
        except Exception as e:
            print(f"Error loading configuration: {e}")

    def upload_file(self, file_name, bucket_name, object_path=None):
        
        if object_path:
            object_key = f"{object_path}/{file_name}"
        else:
            object_key = file_name
        
        self.s3.upload_file(file_name, bucket_name, object_key)
        
        tag_resp=str(input('Press y if you want to tag your object?:'))
        if tag_resp=='y':
            self.Bucket=self.config['bucket_name']
            self.Key=str(input('Please enter key value or path for tag:'))
            self.tag_key=str(input('Please enter key for tag:'))
            self.tag_value=str(input('Please enter value for tag:'))

            try:
                response=self.s3.get_object_tagging(
                    Bucket=self.Bucket,
                    Key=self.Key
                )
                existing_tags=response['TagSet']
                new_tags=[]
                
                for tag in existing_tags:
                    if tag['Key']==self.tag_key:
                        tag['Value']=self.tag_value
                    new_tags.append(tag)
                
                if len(existing_tags)>=10:
                    raise ValueError("Maximum num of tags reached")

                if self.tag_key not in [tag['Key'] for tag in existing_tags]:
                        new_tags.append({'Key':self.tag_key,'Value':self.tag_value})
                
                self.s3.put_object_tagging(
                    Bucket=self.bucket_name,
                    Key=self.Key,
                    Tagging={
                        'TagSet':new_tags                        }
                )
                print("Tag added")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(f"Object with key '{object_key}' not found in bucket '{self.bucket_name}'.")
                else:
                    print(f"Error adding tag: {e}")
            except Exception as ex:
                print(f"Error: {e}")
        
    def download_file(self, bucket_name, object_name, file_name):
        try:
            response=self.s3.list_buckets()
            bucket_exists=False
            for name in response['Buckets']:
                if bucket_name==name['Name']:
                    bucket_exists=True
                    break
            if not bucket_exists:
                print(f"Error:Bucket '{bucket_name}' not found")
                return
            response2=self.s3.list_objects_v2(Bucket=bucket_name)
            object_exists=False
            if 'Contents' in response2:
                for obj in response2['Contents']:
                    if object_name==obj['Key']:
                        object_exists=True
                        break
            if not object_exists:
                print(f"Error: Object with key '{object_name}' not found in bucket '{bucket_name}'.")
                return
            self.s3.download_file(bucket_name, object_name, file_name)
            print("file downloaded in",file_name)
        except ClientError as e:
            if e.response['Error']['Code']=='NoSuchBucket':
                print(f"Error: Bucket '{bucket_name}' not found ")
            elif e.response['Error']['Code']=='NoSuchKey':
                print(f"Object with key '{object_name}' not found in bucket '{bucket_name}'")
            else:
                print(f"Error downloading file:{e}")
        except Exception as ex:
            print(f"Error: {e}")

    def list_files(self, bucket_name):
        try:
            response=self.s3.list_buckets()
            bucket_exists=False
            for name in response['Buckets']:
                if bucket_name==name['Name']:
                    bucket_exists=True
                    break
            if not bucket_exists:
                print(f"Error:Bucket '{bucket_name}' not found")
                return
            response = self.s3.list_objects_v2(Bucket=bucket_name)
            data = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    data.append({
                        "key": obj.get("Key"),
                        #"LastModified": obj.get("LastModified"),
                        "name": response.get("Name"),
                        "prefix": response.get("Prefix"),
                        "KeyCount": response.get("KeyCount"),
                        "MaxKeys":response.get("MaxKeys")
                    })
            return json.dumps(data)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"Error: Bucket '{bucket_name}' not found ")
            else:
                print(f"Error listing objects: {e}")
        except Exception as ex:
            print(f"Error: {e}")

    def delete_file(self, bucket_name, object_name):
        try:
            response=self.s3.list_buckets()
            bucket_exists=False
            for name in response['Buckets']:
                if bucket_name==name['Name']:
                    bucket_exists=True
                    break
            if not bucket_exists:
                print(f"Error:Bucket '{bucket_name}' not found")
                return
            response2=self.s3.list_objects_v2(Bucket=bucket_name)
            object_exists=False
            if 'Contents' in response2:
                for obj in response2['Contents']:
                    if object_name==obj['Key']:
                        object_exists=True
                        break
            if not object_exists:
                print(f"Error: Object with key '{object_name}' not found in bucket '{bucket_name}'.")
                return
            self.s3.delete_object(Bucket=bucket_name, Key=object_name)
            print("File deleted from", bucket_name)        
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"Error: Bucket '{bucket_name}' not found ")
            elif e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Object with key '{object_name}' not found in bucket '{bucket_name}'")
            else:
                print(f"Error deleting file: {e}")
        except Exception as ex:
            print(f"Error: {e}")
        