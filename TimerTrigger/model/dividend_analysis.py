class DividendAnalysis():
    stock_code: str
    company: str
    sector: str
    second_sector: str
    stock_price: float
    stock_type: str
    valuation: float
    stock_available_volume: int
    vol_negotiated_last_21: int
    dividend_last_price: float
    company_net_profit: float
    dividend_yield: float
    dividend_avg_payout_12_mos: float
    dividend_avg_payout_5_yrs: float
    major_share_holder: str
    company_roe: float
    company_roe_5_yrs: float
    comp_grossdebt_ebtida: float
    dividend_yield_5_yrs: float
    has_dividend_srd_5_yrs: int
    has_dividend_grwth_5_yrs: int
    has_net_profit_reg_5_yrs: int

    def __init__(self):
        self.stock_code = ''
        self.company = ''
        self.sector = ''
        self.second_sector = ''
        self.stock_price = 0.00
        self.stock_type = ''
        self.valuation = 0.00
        self.stock_available_volume = 0
        self.vol_negotiated_last_21 = 0
        self.dividend_last_price = 0.00
        self.company_net_profit = 0.00
        self.dividend_yield = 0.00
        self.dividend_avg_payout_12_mos = 0.00
        self.dividend_avg_payout_5_yrs = 0.00
        self.major_share_holder = ''
        self.company_roe = 0.00
        self.company_roe_5_yrs = 0.00
        self.comp_grossdebt_ebtida = 0.00
        self.dividend_yield_5_yrs = 0.00
        self.has_dividend_srd_5_yrs = 0
        self.has_dividend_grwth_5_yrs = 0
        self.has_net_profit_reg_5_yrs = 0
        pass
    pass