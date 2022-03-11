import json
import os
import requests
import sys
import time
import configparser as cp
import logging as log
import datetime as dt
import warnings
from urllib.parse import quote as urlquote

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log.basicConfig(filename=dt.datetime.utcnow().strftime('LsacAuth_%Y%m%d_%H%M%S.log'),
                format='%(asctime)s :%(message)s ', datefmt='%Y%m%d %I:%M:%S %p')


class LsacAuth:

    def __init__(self):
        self.cache = dict()
        self.lsac_access_token_url = os.environ.get("LSAC_ACCESS_TOKEN_URL")
        self.lsac_service_url = os.environ.get("LSAC_SERVICE_URL")
        self.client_id = os.environ.get("LSAC_CLIENT_ID")
        self.client_secret = os.environ.get("LSAC_CLIENT_SECRET")
        self.vendor_id = os.environ.get("VENDOR_ID")
        self.vendor_base_url = os.environ.get("VENDOR_BASE_URL")
        self.Study_id = os.environ.get("STUDY_ID")
        warnings.simplefilter("ignore")

    def lsac_oauth_token(self):
        log.info('starting to get LSAC oauth token')
        querystring = {"grant_type": "client_credentials", "client_id": self.client_id,
                       "client_secret": self.client_secret}
        headers = {'service': self.lsac_service_url}
        response = requests.request("GET", self.lsac_access_token_url, headers=headers, params=querystring,
                                    verify=False)
        try:
            if not response.status_code == 200:
                print('Retrying to get LSAC oauth token...')
                time.sleep(5)
                self.lsac_oauth_token()
            return response.json()
        except Exception as e:
            log.exception(f"""Error occured:{e}""")

    def get_vendor_connection_parameters(self, url_type):
        token = self.lsac_oauth_token()
        h = {"Authorization": "{} {}".format(token['token_type'].capitalize(), token['access_token'])}
        log.info(f"Fetching credentails for {self.vendor_id}")
        vendor_id = urlquote(self.vendor_id)
        conn_name_url = f"""{self.vendor_base_url}/{url_type}""".replace('vendor_id', vendor_id)
        log.info(f"Connection URL is {conn_name_url}")
        print("Getting parameters through LSACAuth from ", conn_name_url)
        conn_name_rs = requests.get(conn_name_url, headers=h, verify=False)
        print(f"""status code for getting vendor cred:{conn_name_rs}""")
        if conn_name_rs.status_code == 200:
            if "no edc connection " not in str(conn_name_rs.content):
                cred = conn_name_rs.json()
                return cred
            else:
                log.info(f""" {self.vendor_id}""")
                sys.exit(f"""{self.vendor_id}""")
        else:
            log.info(conn_name_rs.status_code)
            log.info(f"""Unable to get {self.vendor_id} cred,status code:{conn_name_rs.status_code}""")
            sys.exit(
                f"""Unable to get {self.vendor_id} connection name,status code:{conn_name_rs.status_code},exiting""")

    def main(self, CredType):
        if CredType == 'Vendor':
            log.info(f"Getting information for Vendor : {self.vendor_id}")
            config_param_list = list()
            if self.vendor_id not in self.cache:
                url_type = 'pda-study/api/v1/sourcesystem/thirdparty/vendor/connection/get/details/vendor_id'
                self.cache[self.vendor_id] = self.get_vendor_connection_parameters(url_type)
                config_param_list.append(self.cache[self.vendor_id])
                log.info(f"Parameters fetched for vendor_id {self.vendor_id}")
        if CredType == 'Domain':
            print(f"Getting information for Product")
            config_param_list = list()
            url_type = 'pda-study/api/v1/study/sourcesystem/thirdparty/sourcesetup/ops_vendor_id'
            config_param_list = self.get_vendor_connection_parameters(url_type)[self.vendor_id]
            log.info(f"Parameters fetched for Product")
        return config_param_list


if __name__ == '__main__':
    lsac_obj = LsacAuth()
    print(lsac_obj.main('Domain'))