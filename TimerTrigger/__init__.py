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

SETTINGS_FILE_PATH = pathlib.Path(__file__).parent.parent.__str__() + "//local.settings.json"

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

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
            if ("cvm_code" not in code) or ("stock" not in code):
                continue

            logging.info("Starting '{}'".format(code["stock"]))
            
            dividend_analysis_aux: DividendAnalysis = DividendAnalysis()
            dividend_analysis_aux.stock_code = code["stock"]
            dividend_analysis_aux.stock_type = code["stock_type"] if ("stock_type" in code) else ""
            dividend_analysis_aux.stock_available_volume = int(code["available_volume"]) if ("available_volume" in code) else -1
            dividend_analysis_aux.major_share_holder = ""

            # Company Information
            comp_info = get_node_from_json(compaies_info, "cvm_code", code["cvm_code"])

            if comp_info:
                dividend_analysis_aux.company = comp_info["name"] if ("name" in comp_info) else ""  
                dividend_analysis_aux.sector = comp_info["sector"] if ("sector" in comp_info) else ""
                dividend_analysis_aux.second_sector = comp_info["maj_activity"] if ("maj_activity" in comp_info) else ""
                pass

            # Market History
            stk_mkt_hist = get_node_from_json(stk_mkt_histories, "code", code["stock"])

            if stk_mkt_hist:
                dividend_analysis_aux.stock_price = float(stk_mkt_hist["last_price"]) if ("last_price" in stk_mkt_hist) else -1  
                dividend_analysis_aux.vol_negotiated_last_21 = int(stk_mkt_hist["volume"]) if ("volume" in stk_mkt_hist) else -1  
                pass    
            
            # Dividend History
            divid_hist = get_node_from_json(stk_dividend_histories, "code", code["stock"])

            if divid_hist:
                dividend_analysis_aux.dividend_last_price = float(divid_hist["dividend_last_price"]) if ("dividend_last_price" in divid_hist) else -1
                pass

            # Company Statistics
            comp_stats = get_node_from_json(companies_stats, "code", code["stock"])

            if comp_stats:
                dividend_analysis_aux.valuation = float(comp_stats["valuation"]) if("valuation" in comp_stats) else -1.0    
                dividend_analysis_aux.dividend_yield = float(comp_stats["dividend_yield"]) if("dividend_yield" in comp_stats) else -1.0
                dividend_analysis_aux.dividend_avg_payout_12_mos = float(comp_stats["avg_payout_12_mos"]) if("avg_payout_12_mos" in comp_stats) else -1.0
                dividend_analysis_aux.dividend_avg_payout_5_yrs = float(comp_stats["avg_payout_5_yrs"]) if("avg_payout_5_yrs" in comp_stats) else -1.0
                dividend_analysis_aux.comp_grossdebt_ebtida = float(comp_stats["comp_grossdebt_ebtida"]) if ("comp_grossdebt_ebtida" in comp_stats) else -1.0
                dividend_analysis_aux.dividend_yield_5_yrs = float(comp_stats["dividend_yield_5_yrs"]) if ("dividend_yield_5_yrs" in comp_stats) else -1.0
                dividend_analysis_aux.company_roe = float(comp_stats["company_roe"]) if ("company_roe" in comp_stats) else -1.0
                dividend_analysis_aux.company_roe_5_yrs = float(comp_stats["company_roe_5_yrs"]) if ("company_roe_5_yrs" in comp_stats) else -1.0
                pass

            # Company Financial History
            comp_fin_hist = get_node_from_json(companies_financial_histories, "code", code["stock"])

            if comp_fin_hist:
                dividend_analysis_aux.company_net_profit = float(comp_fin_hist["net_profit"]) if ("net_profit" in comp_fin_hist) else -1.0
                dividend_analysis_aux.has_dividend_srd_5_yrs = int(comp_fin_hist["has_dividend_been_constantly_shared"]) if ("has_dividend_been_constantly_shared" in comp_fin_hist) else 0
                dividend_analysis_aux.has_dividend_grwth_5_yrs = int(comp_fin_hist["has_dividend_grown_over_years"]) if ("has_dividend_grown_over_years" in comp_fin_hist) else 0
                dividend_analysis_aux.has_net_profit_reg_5_yrs = int(comp_fin_hist["has_net_profit_grown_over_years"]) if ("has_net_profit_grown_over_years" in comp_fin_hist) else 0
                pass

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

def get_node_from_json(jsonObj, key, value) -> str:
    if not jsonObj: 
        return None

    for item in jsonObj:
        if key in item:
            if str(item[key]) == str(value):
                return item
    
    return None