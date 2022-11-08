try:
    import boto3
    import csv
    import uuid
    import json
    import datetime
    import re
    from datetime import datetime
    import os
    import io

    from io import StringIO
    import faker
    from faker import Faker
    import uuid

except Exception as e:
    print("Error :{} ".format(e))


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(
            self,
            bucket=os.getenv("REPORTS_BUCKETS"),
            aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
            region_name=os.getenv("DEV_REGION"),
    ):
        self.BucketName = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                ACL="private", Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Failed to upload records. Error : {}".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(
                Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):
        """Gets the Bytes Data from AWS S3 """
        try:
            response_new = self.client.get_object(
                Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()
        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):
        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """
        flag = self.item_exists(Key=key)
        if flag:
            data = self.get_item(Key=key)
            return data
        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):
        response = self.client.delete_object(Bucket=self.BucketName, Key=Key)
        return response

    def get_all_keys(self, Prefix=""):
        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)
            tmp = []
            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])
            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=""):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "


global faker
faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        name = faker.name().split(" ")
        first_name = name[0]
        last_name = name[1]
        address = faker.address()
        text = faker.text()
        id = uuid.uuid4().__str__()
        city = faker.city()
        state = faker.state()

        _ = {
            "first_name": first_name,
            "last_name": last_name,
            "address": address,
            "text": text,
            "id": id,
            "city": city,
            "state": state
        }
        return _


def main():

    """
     CREATE TABLE IF NOT EXISTS public.users
    (
        first_name character varying(256) COLLATE pg_catalog."default",
        last_name character varying(256) COLLATE pg_catalog."default",
        address character varying(256) COLLATE pg_catalog."default",
        text character varying(256) COLLATE pg_catalog."default",
        id character varying(256) COLLATE pg_catalog."default",
        city character varying(256) COLLATE pg_catalog."default",
        state character varying(256) COLLATE pg_catalog."default"
    )

    """

    data = DataGenerator.get_data()
    helper_process_files = AWSS3(
        aws_access_key_id="XXXX",
        aws_secret_access_key="XXX",
        region_name="us-east-1",
        bucket="XXXXX"
    )
    key = f"soumil_data/{uuid.uuid4().__str__()}.json"
    helper_process_files.put_files(
        Key=key, Response=json.dumps(data)
    )
    print(data)
    print("\n")

main()
