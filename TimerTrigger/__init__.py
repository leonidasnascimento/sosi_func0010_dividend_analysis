import datetime
import logging
import azure.functions as func
import json
import requests
import pathlib
import threading

from .model.dividend_analysis import DividendAnalysis
from typing import List
from configuration_manager.reader import reader
from azure.storage.blob import (
    Blob,
    BlockBlobService,
    PublicAccess
)

SETTINGS_FILE_PATH = pathlib.Path(
    __file__).parent.parent.__str__() + "//local.settings.json"

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    try:
        logging.info("'TimerJobSosiMs0010DividendAnalysis' has begun")

        config_obj: reader = reader(SETTINGS_FILE_PATH, 'Values')
        stock_code_list_service_url: str = config_obj.get_value("STOCK_LIST_SERVICE")
        company_info_service_url: str = config_obj.get_value("COMPANY_INFO_DIVIDEND_ANALYSIS_SERVICE")
        stock_mkt_hist_service_url: str = config_obj.get_value("STOCK_MKT_HIST_DIVIDEND_ANALYSIS_SERVICE")
        stock_dividend_hist_service_url: str = config_obj.get_value("STOCK_DIVIDEND_HIST_DIVIDEND_ANALYSIS_SERVICE")
        company_stats_service_url: str = config_obj.get_value("COMPANY_STATS_DIVIDEND_ANALYSIS_SERVICE")
        company_fin_hist_service_url: str = config_obj.get_value("COMPANY_FINANCIAL_HIST_DIVIDEND_ANALYSIS_SERVICE")
        fundamental_anlysis_dividend_service_url: str = config_obj.get_value("FUNDAMENTAL_ANALYSIS_DIVIDENDS_SERVICE")

        if ((not stock_code_list_service_url) or stock_code_list_service_url == ''):
            logging.error("Stock list service url can not be null or empty")
            return

        if ((not company_info_service_url) or company_info_service_url == ''):
            logging.error("Company info service url can not be null or empty")
            return

        if ((not stock_mkt_hist_service_url) or stock_mkt_hist_service_url == ''):
            logging.error("Stock mkt history service url can not be null or empty")
            return

        if ((not stock_dividend_hist_service_url) or stock_dividend_hist_service_url == ''):
            logging.error("Stock dividend history service url can not be null or empty")
            return

        if ((not company_stats_service_url) or company_stats_service_url == ''):
            logging.error("Company stats service url can not be null or empty")
            return

        if ((not company_fin_hist_service_url) or company_fin_hist_service_url == ''):
            logging.error("Company financial history service url can not be null or empty")
            return

        if ((not fundamental_anlysis_dividend_service_url) or fundamental_anlysis_dividend_service_url == ''):
            logging.error("Fundamental analysis service url can not be null or empty")
            return

        logging.info("Getting stock list. It may take a while...")
        with requests.request("GET", stock_code_list_service_url) as response:
            stk_codes = json.loads(response.text)

        logging.info("Getting companies information. It may take a while...")
        with requests.request("GET", company_info_service_url) as response:
            compaies_info = json.loads(response.text)

        logging.info("Getting histories from stock market. It may take a while...")
        with requests.request("GET", stock_mkt_hist_service_url) as response:
            stk_mkt_histories = json.loads(response.text)

        logging.info("Getting histories from stock dividends. It may take a while...")
        with requests.request("GET", stock_dividend_hist_service_url) as response:
            stk_dividend_histories = json.loads(response.text)

        logging.info("Getting companies stats for dividend analysis. It may take a while...")
        with requests.request("GET", company_stats_service_url) as response:
            companies_stats = json.loads(response.text)

        logging.info("Getting companies financial histories. It may take a while...")
        with requests.request("GET", company_fin_hist_service_url) as response:
            companies_financial_histories = json.loads(response.text)

        if (not stk_codes or len(stk_codes) == 0):
            logging.error("No stock codes to process")
            return

        logging.info("Stocks found: {0}".format(len(stk_codes)))

        for code in stk_codes:
            logging.info("Starting '{}'".format(code["stock"]))
            dividend_analysis_aux: DividendAnalysis = DividendAnalysis()
            
            json_obj = json.dumps(dividend_analysis_aux.__dict__)
            threading.Thread(target=post_data, args=(fundamental_anlysis_dividend_service_url, json_obj)).start()

        logging.info("Timer job is done. Waiting for the next execution time")
        pass
    except Exception as ex:
        error_log = '{} -> {}'
        logging.exception(error_log.format(utc_timestamp, str(ex)))
        pass
    pass

def post_data(url, json):
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache"
    }

    requests.request("POST", url, data=json, headers=headers)
    pass