import boto3
import yaml
import json
from botocore.exceptions import NoCredentialsError

class S3():
        
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = None
        self.s3 = None
        self._load_config()
        self.bucket_name=self.config['bucket_name']

    def _load_config(self):
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
            response=self.s3.get_object_tagging(
                Bucket=self.Bucket,
                Key=self.Key
            )
            if 'TagSet' in response:
                tag_set = response['TagSet']
                for tag in tag_set:
                    if tag['Key'] == self.tag_key:
                        print(f"Tag key '{self.tag_key}' exists.")
                        break
                        # self.s3.put_object_tagging(
                        #     Bucket=self.Bucket,
                        #     Key=self.Key,
                        #     Tagging={
                        #         'TagSet': [
                        #              {
                        #                 # 'Key': self.tag_key,
                        #                 'Value': self.tag_value
                        #             },
                        #         ]
                        #     },
                        # )
            else:   
                self.s3.put_object_tagging(
                    Bucket=self.Bucket,
                    Key=self.Key,
                    Tagging={
                        'TagSet': [
                            {
                                'Key': self.tag_key,
                                'Value': self.tag_value
                            },
                        ]
                    },
                )
        
    
    def download_file(self, bucket_name, object_name, file_name):
        self.s3.download_file(bucket_name, object_name, file_name)
        print("file downloaded in",file_name)

    def list_files(self, bucket_name):
        response = self.s3.list_objects_v2(Bucket=bucket_name)
        # if 'Contents' in response:
        #     return [obj['Key'] for obj in response['Contents']]
        # else:
        #     return []
        
        # data={
        #     "key":response.get("Contents",[])[0].get("Key"),
        #     #"LastModified": response.get("Contents", [])[0].get("LastModified"),
        #     "name": response.get("Name"),
        #     "prefix": response.get("Prefix"),
        #     "KeyCount": response.get("KeyCount")
        # }
        # return json.dumps(data)

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


    def delete_file(self, bucket_name, object_name):
        self.s3.delete_object(Bucket=bucket_name, Key=object_name)
        print("file deleted from",bucket_name)