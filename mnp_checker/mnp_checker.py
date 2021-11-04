import os
import sys
import json
from typing import Dict, Any, List, Optional
import dotenv
import aiohttp
import asyncio
import requests
import urllib.parse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv.load_dotenv(os.path.join(BASE_DIR, '.env'))
MNPDB: Optional[str] = os.getenv('DBNAME')
HLR_LOGIN: Optional[str] = os.getenv('HLR_LOGIN')
HLR_PASS: Optional[str] = os.getenv('HLR_PASS')
HLR_URL: Optional[str] = os.getenv('HLR_URL')
SMS_USERNAME: Optional[str] = os.getenv('SMS_USERNAME')
SMS_PASS: Optional[str] = os.getenv('SMS_PASS')
SMS_URL: Optional[str] = os.getenv('SMS_URL')
SMS_MSISDN: Optional[str] = os.getenv('SMS_MSISDN')

if MNPDB is None:
    print(f'set up DBNAME in {BASE_DIR}/.env')
    sys.exit()

if HLR_URL is None:
    print(f'set up URL in {BASE_DIR}/.env')
    sys.exit()


class MnpChecker:

    def __init__(self, db: str) -> None:
        self.db = db

    def read_db(self) -> Dict[str, Dict[Any, Any]]:
        with open(self.db, 'r') as f:
            json_db = json.load(f)
        return json_db

    async def send_hlr_request(self, msisdns: List[str]) -> Dict[str, Any]:
        response = {}
        async with aiohttp.ClientSession() as session:
            for msisdn in msisdns:
                url = HLR_URL.format(HLR_LOGIN, HLR_PASS, msisdn)
                async with session.get(url) as resp:
                    msisdn_info = await resp.json()
                    response[msisdn] = msisdn_info
        return response

    def get_country_from_db(self, db: Dict[Any, Any]) -> List[str]:
        return list(db.keys())

    def parse_hlr_response(self, response: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        # response example
        # {'79216503431': {'source': 'MNP',
        #                  'ported': 1,
        #                  'ownerID':
        #                  'mTINKOFF',
        #                  'mccmnc': '250062',
        #                  'source_name': 'mnp',
        #                  'dnis': '79216503431',
        #                  'source_type': 'mnp',
        #                  'id': 59554872,
        #                  'result': 0,
        #                  'cached': 0}}
        parsed_resp = {key:
                           {'mccmnc': response[key].get('mccmnc'),
                             'ownerID': response[key].get('ownerID')}
                       for key in list(response.keys())}
        return parsed_resp

    def is_db_equal(self, mnp_db: Dict[Any, Any], hlr_db: Dict[Any, Any], country: str) -> bool:
        if mnp_db[country] == hlr_db[country]:
            return True
        return False

    def get_db_diff(self, mnp_db: Dict[str, Dict], hlr_db: Dict[str, Dict]) -> List[str]:
        diff = []
        for msisdn in mnp_db:
            mnp_mccmnc = mnp_db[msisdn].get('mccmnc')
            mnp_ownerid =  mnp_db[msisdn].get('ownerID')
            hlr_mccmnc = hlr_db[msisdn].get('mccmnc')
            hlr_ownerid = hlr_db[msisdn].get('ownerID')
            if mnp_mccmnc != hlr_mccmnc:
                diff.append(f'{msisdn} hlr_mccmnc - {hlr_mccmnc} expected {mnp_mccmnc}')
            if mnp_ownerid != hlr_ownerid:
               diff.append(f'{msisdn} hlr_ownerid - {hlr_ownerid} expected {mnp_ownerid}')
        return diff

    def send_sms_alarm(self, text: str) -> None:
        requests.get(SMS_URL.format(SMS_USERNAME, SMS_PASS, SMS_MSISDN, text))


if __name__ == '__main__':
    db_path = os.path.join(BASE_DIR, MNPDB)
    mnp = MnpChecker(db=db_path)
    mnp_db = mnp.read_db()
    mnp_countries = mnp.get_country_from_db(mnp_db)
    hlr_db = {}
    for country in mnp_countries:
        msisdns: List = list(mnp_db[country].keys())
        hlr_resp = asyncio.run(mnp.send_hlr_request(msisdns))
        hlr_db[country] = mnp.parse_hlr_response(hlr_resp)
    check_result: Dict[str, bool] = {}
    for country in mnp_countries:
        check_result[country] = mnp.is_db_equal(hlr_db, mnp_db, country)

    if False in check_result.values():
        sms_text: str = ''
        countries = [country for country, value in check_result.items() if value is False]
        for country in countries:
            diff = mnp.get_db_diff(hlr_db[country], mnp_db[country])
            sms_text += '\n'.join(diff)
            sms_text += '\n'
        sms_text = urllib.parse.quote_plus(sms_text)
        mnp.send_sms_alarm(sms_text)
