const date_int_cols = Set(Symbol(c) for c in (
    "Estimate Arrival Date",
    "Actual Arrival Date",
))

const categoricals = Set(Symbol(c) for c in (
    "Bill Type Code",
    "Carrier SASC Code",
    "Vessel Country Code",
    "Vessel Code",
    "Vessel Name",
    "Loading Port",
    "Last Vist Foreign Port",
    "US Clearing District",
    "Unloading Port",
    "Country",
    "Weight Unit",
    "Quantity Unit",
    "Measure Unit",
    "Container Desc Code",
    "Container Load Status",
    "Container Type of Service",
    "HS Code",
    "HS Code Sure Level",
    "Indicator of true supplier",
    "Indicator of true buyer",
    "END",
    "HS_Code",
    "US_Exporter",
    "Country_of_Foreign_Port",
    "Quantity_Unit",
    "Weight_Unit",
    "Carrier_Code",
    "Foreign_Port",
))

const date_regex = r"[D,d]ate"

# https://oec.world/api/olap-proxy-python/data.parquet?cube=bill_of_lading_explorer&locale=en&drilldowns=Vessel+Country%2CCountry%2CYear%2CEstimate+Arrival+Year%2CActual+Arrival+Year%2CUnloading+Port%2CSubnat+Geography&measures=Count&limit=99999999%2C0&token={TOKEN}

const domain = "oec.world"

# const olap_proxy_url = joinpath(api_url, "olap-proxy-python")

# const dload_proxy_url = joinpath(api_url, "download-proxy")

# const bulk_bol_usa_exports_url_path = joinpath(dload_proxy_url, "bill_of_lading/BOL USA/Exports")
