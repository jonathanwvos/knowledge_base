import boto3

IMAGE_EXTENSIONS = ('jpg', 'JPG', 'JPEG', 'jpeg', 'png', 'PNG')

class S3DataImporter:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')

    def get_image_keys(self, prefix):
        start_key = ''

        keys = []
        while(True):
            object_list = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=1000,
                StartAfter=start_key
            )

            if object_list['KeyCount'] == 0:
                break;

            contents = object_list['Contents']

            keys.extend([i['Key'] for i in contents])
            start_key = contents[-1]['Key']

        image_keys = [i for i in keys if i.endswith(IMAGE_EXTENSIONS)]

        return image_keys
