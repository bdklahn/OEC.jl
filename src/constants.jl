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
