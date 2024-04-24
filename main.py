from s3 import S3
import boto3
import configparser
import yaml

with open('config.yaml','r') as file:
    data=yaml.safe_load(file)
    
def main():
    s3_client = S3('config.yaml')

    bucket_name = data['bucket_name']
    file_name = 'new.txt'
    file_to_download = 'downloaded_file'
    object_path = 'new.txt'

    # Upload file
    #s3_client.upload_file(file_name, bucket_name, object_path)
    
    # Download file
    s3_client.download_file(bucket_name, f"{object_path}/{file_name}", file_to_download)

    # List files
    #files = s3_client.list_files(bucket_name)
    #print("Files in bucket:", files)

    # Delete file
    #s3_client.delete_file(bucket_name, f"{object_path}/{file_name}")

if __name__ == "__main__":
    main()