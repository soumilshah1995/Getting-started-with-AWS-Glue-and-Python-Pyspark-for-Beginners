try:
    import pandas as pd
    import boto3
    import os
    from dotenv import load_dotenv

    load_dotenv(".env")
except Exception as e:
    print("Error : {} ".format(e))



class PruneGlueVersion(object):

    def __init__(self,
                 catalog_id,
                 db_name,
                 table_name,
                 aws_access_key_id,
                 aws_secret_access_key,
                 region_name
                 ):
        self.catalog_id = catalog_id
        self.db_name = db_name
        self.table_name = table_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        self.versions_to_compare = [0, 1]
        self.delete_old_versions = False
        self.number_of_versions_to_retain = 1
        self.columns_modified = []

        self.glue = boto3.client('glue',
                                 aws_access_key_id=self.aws_access_key_id,
                                 aws_secret_access_key=self.aws_secret_access_key,
                                 region_name=self.region_name
                                 )

    def prune(self):
        response = self.__get_table_versions()
        table_versions = response['TableVersions']
        table_versions.sort(key=self.__version_id, reverse=True)
        version_count = len(table_versions)
        self.__delele_versions(self.glue, table_versions, self.number_of_versions_to_retain)
        return True

    def __delele_versions(self, glue_client, versions_list, number_of_versions_to_retain):
        print("deleting old versions...")
        if len(versions_list) > number_of_versions_to_retain:
            version_id_list = []
            for table_version in versions_list:
                version_id_list.append(int(table_version['VersionId']))
            # Sort the versions in descending order
            version_id_list.sort(reverse=True)
            versions_str_list = [str(x) for x in version_id_list]
            versions_to_delete = versions_str_list[number_of_versions_to_retain:]

            del_response = glue_client.batch_delete_table_version(
                DatabaseName=self.db_name,
                TableName=self.table_name,
                VersionIds=versions_to_delete
            )
            return del_response

    def __get_table_versions(self):

        response = self.glue.get_table_versions(
            CatalogId=self.catalog_id,
            DatabaseName=self.db_name,
            TableName=self.table_name,
            MaxResults=100
        )
        return response

    def __version_id(self, json):
        try:
            return int(json['VersionId'])
        except KeyError:
            return 0

def main():
    helper = PruneGlueVersion(
        aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
        region_name=os.getenv("DEV_REGION_KEY"),
        catalog_id=os.getenv("ACCOUNT"),
        db_name='XXX',
        table_name='XXX'
    )
    response = helper.prune()
    print(response)


main()
