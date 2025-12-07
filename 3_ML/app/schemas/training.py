from pydantic import BaseModel, Field, conlist
from typing import Optional, Annotated, List
from enum import Enum


class GridEnum(str, Enum):
    quick = "quick"
    balanced = "balanced"
    full = "full"

class RunModeEnum(str, Enum):
    simple = "simple"
    grid = "grid"
    both = "both"

class AllowedModelsEnum(str, Enum):
    LinearRegression = "LinearRegression"
    RandomForest = "RandomForest"
    DecisionTreeRegressor = "DecisionTreeRegressor"
    RandomForestRegressor = "RandomForestRegressor"

    XGBRegressor = "XGBRegressor"
    DecisionTree = "DecisionTree"
    Logistic_OVO = "Logistic_OVO"
    Logistic_OVR = "Logistic_OVR"



class PayloadTrainingParameters(BaseModel):
    """
    Parameters for ML training pipeline
    """

    # --- Folders ---

    DATA_DIR : str = Field(..., description="Folder containing the data files")
    OUTPUT_DIR: str = Field(..., description="Folder where to output the ML results")


    # --- Data & filtering ---
    RUN_MODE: Annotated[
        list[RunModeEnum],
        Field(min_items=1, max_items=1, description="Run mode, grid or simple")
    ]
    airports_optional: Optional[List[str]] = Field(None, description="Optional airports filter")
    airports_mandatory: Optional[List[str]] = Field(None, description="Mandatory airports filter")
    RECORD_LIMIT: Optional[str] = Field(None, description="Limit number of records for testing purposes")

    # --- Feature selection & model ---
    TOP_K_FEATURES: int = Field(..., description="Number of top features to keep")


    GRID_LEVEL: Annotated[
            list[GridEnum],
            Field(min_items=1, max_items=1, description="Level of hyperparameter search, e.g., 'quick', 'extended'")
            ]    
    PARALLEL_JOBS: int = Field(..., description="Number of parallel jobs for model training")
    MODEL_LIST_TO_TEST: List[AllowedModelsEnum] = Field(..., description="List of models to test")

    # --- Target columns ---
    TARGET_REGRESSION: str = Field(..., description="Target column for regression delay prediction")
    TARGET_CLASSIFICATION_STATUS: str = Field(..., description="Target column for flight status classification")
    TARGET_CLASSIFICATION_DELAY: str = Field(..., description="Target column for delay range classification")

    # --- Training parameters ---
    CV_NB: int = Field(..., description="Number of cross-validation folds")
    TEST_SIZE: float = Field(..., description="Proportion of dataset for test split")
    RANDOM_STATE: int = Field(..., description="Random seed for reproducibility")

    # --- Columns to drop/keep ---
    columnKeywordsToDrop_all: Optional[str] = Field(None, description="Column keywords to drop for all models")
    columnKeywordsToKeep_classification_status: Optional[str] = Field(None, description="Column keywords to keep for classification status")
    columnKeywordsToDrop_classification_status: Optional[str] = Field(None, description="Column keywords to drop for classification status")
    columnKeywordsToDrop_classification_delay: Optional[str] = Field(None, description="Column keywords to drop for delay classification")
    columnKeywordsToDrop_regression: Optional[str] = Field(None, description="Column keywords to drop for regression")
    columnsToDrop_classification_status: Optional[str] = Field(None, description="Explicit columns to drop for classification status")
    columnsToDrop_classification_delay: Optional[str] = Field(None, description="Explicit columns to drop for delay classification")
    columnsToDrop_regression: Optional[str] = Field(None, description="Explicit columns to drop for regression")

    class Config:
        extra = "allow"  # Keeps compatibility with extra fields if needed
        schema_extra = {
            "example": 
{
"RUN_MODE" : "simple",     
"TOP_K_FEATURES" : 20,
"GRID_LEVEL" : "quick",  
"PARALLEL_JOBS" : 6,
"TEST_SIZE" : 0.2,
"RANDOM_STATE" : 42,
"RECORD_LIMIT" : "", 
"TARGET_REGRESSION" : "flightlegs_irregularity_delayduration_total",
"TARGET_CLASSIFICATION_STATUS" : "flightlegs_publishedstatus",
"TARGET_CLASSIFICATION_DELAY" : "flightlegs_irregularity_delayduration_total_bracket",
"MODEL_LIST_TO_TEST" : [
    "LinearRegression",
    "RandomForest",
    "DecisionTreeRegressor",
    "RandomForestRegressor",
    "XGBRegressor",
    "DecisionTree",
    "Logistic_OVO",
    "Logistic_OVR"
    ], 
"CV_NB" : 5,
"columnKeywordsToDrop_all" : ["id",

                            "airline_name",
                            "flightlegs_aircraft_ownerairlineCode",
                            "actual",
                            "posterm",
                            "latestpublished",
                            "airline_code",
                            "company_flight",
                            "iata",
                            "icao",
                            "city_country_areaCode",
                            "airport_location",
                            "airport_city_country_name",
                            "delayreason"],



"columnKeywordsToKeep_classification_status" : [
      "flightlegs_aircraft_ownerairlinecode", 
"flightlegs_aircraft_typecode",
 "flightlegs_depinfo_airport_country_name", 
 "flightlegs_arrinfo_airport_code", 
 "flightlegs_arrinfo_airport_country_name",
  "flightlegs_depinfo_airport_code",
   "flightlegs_scheduledflightduration",
      "flightlegs_season", 
      "flightlegs_arrinfo_times_scheduled_dayPeriod",
       "flightlegs_depinfo_times_scheduled_dayPeriod"],
                            
"columnKeywordsToDrop_classification_status" : ["delay",
                                       "country_code",
                                       "flightNumber",
                                       "flightlegs_legstatuspublic",
                                       "airline_name",
                                       "flightlegs_serviceType",
                                       "status",
                                       "status",
                                       "estimated"],

"columnKeywordsToDrop_classification_delay" : ["country_code",
                                   "flightNumber",
                                   "flightlegs_legdelaypublic",
                                   "airline_name",
                                   "flightlegs_serviceType",
                                   "estimated",
                                   "irregularity_delayInformation",     
                                   "flightlegs_category",
                                   "flightstatuspublic",
                                   "status",
                                   "status",
                                   "flightlegs_irregularity_delayReason"],

"columnKeywordsToDrop_regression" : ["country_code",
                                   "flightNumber",
                                   "flightLegs_legdelayPublic",
                                   "airline_name",
                                   "flightLegs_serviceType",
                                   "estimated",
                                   "irregularity_delayInformation",     
                                   "flightLegs_Category",
                                   "flightStatusPublic",
                                   "status",
                                   "Status",
                                   "flightLegs_irregularity_delayReason"],
                                   
"columnsToDrop_classification_status" : ["flightlegs_arrinfo_times_scheduled",
                                "flightlegs_departureInformation_times_scheduled"],
"columnsToDrop_classification_delay" : ["flightlegs_arrinfo_times_scheduled",
                            "flightlegs_departureInformation_times_scheduled",
                            "flightlegs_irregularity_delayduration"],
"columnsToDrop_regression" : ["flightLegs_arrivalInformation_times_scheduled",
                            "flightLegs_departureInformation_times_scheduled",
                            "flightLegs_irregularity_delayDuration"],
                            
"DATA_DIR" : "data",
"OUTPUT_DIR": "outputs",
"airports_mandatory" : ["CDG","AMS","ORY","FCO","LHR","CPH","MAD","ARN","OSL","LIN","NCE","BCN","LYS","BGO","LIS","DUB","HEL","TLS","OTP","FRA","MRS","ATH","PMI","MUC","TRD","MAN","BER","AGP","OPO"],

    "airports_optional" : ["BHX","BOH","BRS","EXT","HUY","LBA","LPL","LGW","LHR","LCY","SEN","STN","LTN","MAN","MME","NCL","NQY","NWI","EMA","SOU","BFS","BHD","LDY","ABZ","EDI","GLA","PIK","INV","CWL","ANR","BRU","CRL","LGG","OST","AJA","BIA","BVA","EGC","BZR","BIQ","BOD","BES","CCF","XCR","CMF","DNR","FSC","GNB","LRH","LIL","LIG","LYS","MRS","BSL","NTE","NCE","FNI","CDG","ORY","PUF","PGF","PIS","RDZ","EBU","SXB","TLN","TLS","TUF","GIB","ORK","DUB","KIR","NOC","SNN","IOM","JER","LUX","AMS","EIN","GRQ","MST","RTM","GRZ","KLU","INN","LNZ","SZG","VIE","BRQ","JCL","KLV","OSR","PED","PRG","FKB","BER","BRE","CGN","DTM","DUS","FRA","HHN","FDH","HAM","HAJ","LEJ","LBC","FMM","MUC","NUE","STR","NRN","BUD","DEB","SOB","BZG","GDN","KTW","KRK","LUZ","LCJ","SZY","POZ","RZE","SZZ","WAW","WMI","RDO","WRO","BTS","KSC","PZY","TAT","ILZ","BSL","BRN","GVA","LUG","ACH","ZRH","BWK","DBV","LSZ","OSI","PUY","RJK","SPU","ZAD","ZAG","ATH","EFL","CHQ","JKH","CFU","HER","KLX","AOK","KVA","KGS","JMK","MJT","PVK","RHO","SMI","JTR","JSI","SKU","SKG","VOL","ZTH","AHO","AOI","BRI","BGY","BLQ","VBS","BDS","CAG","CTA","CUF","FLR","GOA","SUF","LIN","MXP","NAP","OLB","PMO","PMF","PEG","PSR","PSA","RMI","FCO","CIA","QSR","TPS","TRS","TRN","VCE","VRN","MLA","BYJ","FAO","FNC","LIS","PDL","OPO","PXO","TER","LJU","MBX","POW","LCG","ALC","LEI","OVD","BCN","BIO","CDT","FUE","GRO","LPA","GRX","HSK","IBZ","XRY","SPC","ACE","ILD","MAD","AGP","MAH","RMU","PMI","PNA","REU","SDR","SCQ","SVQ","TFN","TFS","VLC","VLL","VGO","VIT","ZAZ","TIA","GNA","GME","MSQ","BNX","OMO","SJJ","TZL","BOJ","PDV","SOF","VAR","PRN","RMO","ARW","BCM","BAY","GHV","OTP","BBU","CLJ","CND","CRA","IAS","OMR","SUJ","SBZ","SCV","TGM","TSR","TGD","TIV","OHD","SKP","ABA","DYR","AAQ","ARH","ASF","BAX","EGO","BQS","BTK","BZK","CSY","CEK","CEE","HTA","ESL","GRV","IKT","KGD","KZN","KHV","KXK","KRR","KJA","URS","GDX","MQF","MCX","MRV","DME","ZIA","SVO","VKO","MMK","NAL","NBC","NJC","GOJ","NOZ","OVB","OMS","REN","OSW","PEE","PES","PVS","PKC","PKV","ROV","LED","KUF","GSV","AER","STW","SGC","SCW","TOF","TJM","UUD","ULV","UFA","VVO","OGZ","VOG","VOZ","YKS","IAR","SVX","UUS","BEG","KVO","INI","CWC","IFO","HRK","KWG","KBP","IEV","LWO","NLV","ODS","PLV","SIP","UDJ","OZH","AAL","AAR","BLL","CPH","EPU","TLL","TAY","FAE","MHQ","HEL","KTT","KUO","KAO","LPP","OUL","RVN","SVL","TMP","TKU","VAA","AEY","EGS","KEF","RKV","RIX","VNT","KUN","PLQ","SQQ","VNO","AES","BGO","BOO","HAU","KRS","KSU","OSL","TRF","SVG","TOS","TRD","GOT","LLA","MMX","NRK","OSD","ARN","BMA","NYO","VST","SDL","UME","VXO","VBY"]
}        }
