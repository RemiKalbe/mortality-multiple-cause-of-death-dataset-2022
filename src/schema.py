from collections.abc import Mapping
from typing import Optional
import polars as pl


record_type_mapping = {1: "RESIDENTS", 2: "NONRESIDENTS"}
record_type = pl.Enum([v for v in record_type_mapping.values()])
#
# -----------------------------------
#
resident_status_mapping = {
    1: "RESIDENT",
    2: "INTRASTATE_NONRESIDENT",
    3: "INTERSTATE_NONRESIDENT",
    4: "FOREIGN_RESIDENT",
}
resident_status = pl.Enum([v for v in resident_status_mapping.values()])
#
# -----------------------------------
#
education_mapping = {
    1: "8TH_GRADE_OR_LESS",
    2: "9_12TH_GRADE_NO_DIPLOMA",
    3: "HIGH_SCHOOL_GRADUATE_OR_GED_COMPLETED",
    4: "SOME_COLLEGE_CREDIT_NO_DEGREE",
    5: "ASSOCIATE_DEGREE",
    6: "BACHELOR_DEGREE",
    7: "MASTER_DEGREE",
    8: "DOCTORATE_DEGREE_OR_PROFESSIONAL_DEGREE",
    9: None,
}
education = pl.Enum([v for v in education_mapping.values() if v is not None])
#
# -----------------------------------
#
education_reporting_flag_mapping = {
    1: "2003_REVISION",
    2: "NO_EDUCATION_ITEM_ON_CERTIFICATE",
}
education_reporting_flag = pl.Enum(
    [v for v in education_reporting_flag_mapping.values()]
)
#
# -----------------------------------
#
month_of_death = pl.Int8()
#
# -----------------------------------
#
sex_mapping = {"M": "M", "F": "F"}
sex = pl.Enum([v for v in sex_mapping.values()])
#
# -----------------------------------
#
age_lower_bound = pl.Duration(time_unit="ms")
age_upper_bound = pl.Duration(time_unit="ms")
#
# -----------------------------------
#
place_of_death_mapping = {
    1: "HOSPITAL_CLINIC_OR_MEDICAL_CENTER_INPATIENT",
    2: "HOSPITAL_CLINIC_OR_MEDICAL_CENTER_OUTPATIENT_OR_ER",
    3: "HOSPITAL_CLINIC_OR_MEDICAL_CENTER_DEAD_ON_ARRIVAL",
    4: "DECEDENT_S_HOME",
    5: "HOSPICE_FACILITY",
    6: "NURSING_HOME_LONG_TERM_CARE",
    7: "OTHER",
    9: None,
}
place_of_death = pl.Enum([v for v in place_of_death_mapping.values() if v is not None])
#
# -----------------------------------
#
marital_status_mapping = {
    "S": "NEVER_MARRIED_SINGLE",
    "M": "MARRIED",
    "W": "WIDOWED",
    "D": "DIVORCED",
    "U": None,
}
marital_status = pl.Enum([v for v in marital_status_mapping.values() if v is not None])
#
# -----------------------------------
#
day_of_week_of_death_mapping = {
    1: "SUNDAY",
    2: "MONDAY",
    3: "TUESDAY",
    4: "WEDNESDAY",
    5: "THURSDAY",
    6: "FRIDAY",
    7: "SATURDAY",
    9: None,
}
day_of_week_of_death = pl.Enum(
    [v for v in day_of_week_of_death_mapping.values() if v is not None]
)
#
# -----------------------------------
#
injury_at_work_mapping = {"Y": True, "N": False, "U": None}
injury_at_work = pl.Boolean()
#
# -----------------------------------
#
manner_of_death_mapping = {
    1: "ACCIDENT",
    2: "SUICIDE",
    3: "HOMICIDE",
    4: "PENDING_INVESTIGATION",
    5: "COULD_NOT_DETERMINE",
    6: "SELF_INFLICTED",
    7: "NATURAL",
}
manner_of_death = pl.Enum([v for v in manner_of_death_mapping.values()])
#
# -----------------------------------
#
method_of_disposition_mapping = {
    "B": "BURIAL",
    "C": "CREMATION",
    "D": "DONATION",
    "E": "ENTOMBMENT",
    "O": "OTHER",
    "R": "REMOVAL_FROM_JURISDICTION",
    "U": None,
}
method_of_disposition = pl.Enum(
    [v for v in method_of_disposition_mapping.values() if v is not None]
)
#
# -----------------------------------
#
autopsy_mapping = {"Y": True, "N": False, "U": None}
autopsy = pl.Boolean()
#
# -----------------------------------
#
activity_code_mapping = {
    0: "ENGAGED_IN_SPORTS",
    1: "ENGAGED_IN_LEISURE_ACTIVITY",
    2: "WORKING_FOR_INCOME",
    3: "ENGAGED_IN_OTHER_TYPES_OF_WORK",
    4: "VITAL_ACTIVITIES",
    8: None,
    9: None,
}
activity_code = pl.Enum([v for v in activity_code_mapping.values() if v is not None])
#
# -----------------------------------
#
place_of_injury_mapping = {
    0: "HOME",
    1: "RESIDENTIAL_INSTITUTION",
    2: "INSTITUTION_OR_PUBLIC_ADMINISTRATIVE_AREA",
    3: "SPORTS_AND_ATTHLETICS_AREA",
    4: "STREET_AND_HIGHWAY",
    5: "TRADE_AND_SERVICE_AREA",
    6: "INDUSTRIAL_AND_CONSTRUCTION_AREA",
    7: "FARM",
    8: None,
    9: None,
}
place_of_injury = pl.Enum(
    [v for v in place_of_injury_mapping.values() if v is not None]
)
#
# -----------------------------------
#
icd_code = pl.Categorical()
#
# -----------------------------------
#
cause_recode_358 = pl.Categorical()
#
# -----------------------------------
#
cause_recode_113 = pl.Categorical()
#
# -----------------------------------
#
infant_cause_recode_130 = pl.Categorical()
#
# -----------------------------------
#
cause_recode_39 = pl.Int8()


#
# -----------------------------------
#
certificate_part = pl.Enum(["PART_I", "PART_II"])
entity_axis_condition = pl.Struct(
    {
        "certificate_part": pl.Categorical(),
        "certificate_line": pl.Int8(),
        "condition": pl.Categorical(),
    }
)
entity_axis_conditions = pl.List(inner=entity_axis_condition)
#
# -----------------------------------
#
record_axis_conditions = pl.List(inner=pl.Categorical())
#
# -----------------------------------
#
race_recode_6_mapping = {
    1: "WHITE",
    2: "BLACK",
    3: "AMERICAN_INDIAN_AND_ALASKA_NATIVE",
    4: "ASIAN",
    5: "NATIVE_HAWAIIAN_AND_OTHER_PACIFIC_ISLANDER",
    6: "MORE_THAN_ONE_R",
}
race_recode_6 = pl.Enum([v for v in race_recode_6_mapping.values()])
#
# -----------------------------------
#
hispanic_origin_mapping: dict[tuple[int, int], Optional[str]] = {
    (100, 199): "NON_HISPANIC",
    (200, 209): "SPANIARD",
    (210, 219): "MEXICAN",
    (220, 230): "CENTRAL_AMERICAN",
    (231, 249): "SOUTH_AMERICAN",
    (250, 259): "LATIN_AMERICAN",
    (260, 269): "PUERTO_RICAN",
    (270, 274): "CUBAN",
    (275, 279): "DOMINICAN",
    (280, 299): "OTHER_HISPANIC",
    (996, 999): None,
}
hispanic_origin = pl.Enum(
    [v for v in hispanic_origin_mapping.values() if v is not None]
)
#
# -----------------------------------
#
hispanic_origin_race_recode_mapping = {
    1: "MEXICAN",
    2: "PUERTO_RICAN",
    3: "CUBAN",
    4: "DOMINICAN",
    5: "CENTRAL_AMERICAN",
    6: "SOUTH_AMERICAN",
    7: "OTHER_HISPANIC",
    8: "NON_HISPANIC_WHITE",
    9: "NON_HISPANIC_BLACK",
    10: "NON_HISPANIC_AMERICAN_INDIAN_AND_ALASKA_NATIVE",
    11: "NON_HISPANIC_ASIAN",
    12: "NON_HISPANIC_NATIVE_HAWAIIAN_AND_OTHER_PACIFIC_ISLANDER",
    13: "NON_HISPANIC_MORE_THAN_ONE_R",
    14: None,
}
hispanic_origin_race_recode = pl.Enum(
    [v for v in hispanic_origin_race_recode_mapping.values() if v is not None]
)
#
# -----------------------------------
#
race_recode_40_mapping = {
    1: "WHITE",
    2: "BLACK",
    3: "AIAN",
    4: "ASIAN_INDIAN",
    5: "CHINESE",
    6: "FILIPINO",
    7: "JAPANESE",
    8: "KOREAN",
    9: "VIETNAMESE",
    10: "OTHER_ASIAN",
    11: "HAWAIIAN",
    12: "GUAMANIAN",
    13: "SAMOAN",
    14: "OTHER_PACIFIC_ISLANDER",
    15: "BLACK_WHITE",
    16: "BLACK_AIAN",
    17: "BLACK_ASIAN",
    18: "BLACK_NHOPI",
    19: "AIAN_WHITE",
    20: "AIAN_ASIAN",
    21: "AIAN_NHOPI",
    22: "ASIAN_WHITE",
    23: "ASIAN_NHOPI",
    24: "NHOPI_WHITE",
    25: "BLACK_AIAN_WHITE",
    26: "BLACK_AIAN_ASIAN",
    27: "BLACK_AIAN_NHOPI",
    28: "BLACK_ASIAN_WHITE",
    29: "BLACK_ASIAN_NHOPI",
    30: "BLACK_NHOPI_WHITE",
    31: "AIAN_ASIAN_WHITE",
    32: "AIAN_NHOPI_WHITE",
    33: "AIAN_ASIAN_NHOPI",
    34: "ASIAN_NHOPI_WHITE",
    35: "BLACK_AIAN_ASIAN_WHITE",
    36: "BLACK_AIAN_ASIAN_NHOPI",
    37: "BLACK_AIAN_NHOPI_WHITE",
    38: "BLACK_ASIAN_NHOPI_WHITE",
    39: "AIAN_ASIAN_NHOPI_WHITE",
    40: "BLACK_AIAN_ASIAN_NHOPI_WHITE",
}
race_recode_40 = pl.List(
    pl.Enum([v for v in list(race_recode_40_mapping.values())[:15]])
)
#
# -----------------------------------
#
decedent_occupation_code = pl.Categorical()
#
# -----------------------------------
#
decedent_occupation_recode_mapping = {
    1: "MANAGEMENT",
    2: "BUSINESS_AND_FINANCIAL_OPERATIONS",
    3: "COMPUTATIONAL_AND_MATHEMATICAL",
    4: "ARCHITECTURE_AND_ENGINEERING",
    5: "LIFE_PHYSICAL_AND_SOCIAL_SCIENCE",
    6: "COMMUNITY_AND_SOCIAL_SERVICES",
    7: "LEGAL",
    8: "EDUCATION_TRAINING_AND_LIBRARY",
    9: "ARTS_DESIGN_ENTERTAINMENT_SPORTS_AND_MEDIA",
    10: "HEALTHCARE_PRACTITIONERS_AND_TECHNICAL",
    11: "HEALTHCARE_SUPPORT",
    12: "PROTECTIVE_SERVICE",
    13: "FOOD_PREPARATION_AND_SERVING",
    14: "BUILDING_AND_GROUNDS_CLEANING_AND_MAINTENANCE",
    15: "PERSONAL_CARE_AND_SERVICE",
    16: "SALES_AND_RELATED",
    17: "OFFICE_AND_ADMINISTRATIVE_SUPPORT",
    18: "FARMING_FISHING_AND_FORESTRY",
    19: "CONSTRUCTION_AND_EXTRACTION",
    20: "INSTALLATION_MAINTENANCE_AND_REPAIR",
    21: "PRODUCTION",
    22: "TRANSPORTATION_AND_MATERIAL_MOVING",
    24: "MILITARY",
    25: None,
    26: "HOUSEWIFE",
}
decedent_occupation_recode = pl.Enum(
    [v for v in decedent_occupation_recode_mapping.values() if v is not None]
)
#
# -----------------------------------
#
decedent_industry_code = pl.Categorical()
#
# -----------------------------------
#
decedent_industry_recode_mapping = {
    1: "AGRICULTURE_FORESTRY_FISHING_AND_HUNTING",
    2: "MINING",
    3: "UTILITIES",
    4: "CONSTRUCTION",
    5: "MANUFACTURING",
    6: "WHOLESALE_TRADE",
    7: "RETAIL_TRADE",
    8: "TRANSPORTATION_AND_WAREHOUSING",
    9: "INFORMATION",
    10: "FINANCE_AND_INSURANCE",
    11: "REAL_ESTATE_RENTAL_AND_LEASING",
    12: "PROFESSIONAL_SCIENTIFIC_AND_TECHNICAL_SERVICES",
    13: "MANAGEMENT_OF_COMPANIES_AND_ENTERPRISES",
    14: "ADMINISTRATIVE_AND_SUPPORT_AND_WASTE_MANAGEMENT_AND_REMEDIATION_SERVICES",
    15: "EDUCATIONAL_SERVICES",
    16: "HEALTH_CARE_AND_SOCIAL_ASSISTANCE",
    17: "ARTS_ENTERTAINMENT_AND_RECREATION",
    18: "ACCOMMODATION_AND_FOOD_SERVICES",
    19: "OTHER_SERVICES_EXCEPT_PUBLIC_ADMINISTRATION",
    20: "PUBLIC_ADMINISTRATION",
    22: "MILITARY",
    23: None,
}
decedent_industry_recode = pl.Enum(
    [v for v in decedent_industry_recode_mapping.values() if v is not None]
)
#
# -----------------------------------
# -----------------------------------
#
dataset_schema: Mapping[str, pl.PolarsDataType] = {
    "record_type": record_type,
    "resident_status": resident_status,
    "education": education,
    "month_of_death": month_of_death,
    "sex": sex,
    "age_lower_bound": age_lower_bound,
    "age_upper_bound": age_upper_bound,
    "place_of_death": place_of_death,
    "marital_status": marital_status,
    "day_of_week_of_death": day_of_week_of_death,
    "injury_at_work": injury_at_work,
    "manner_of_death": manner_of_death,
    "method_of_disposition": method_of_disposition,
    "autopsy": autopsy,
    "activity_code": activity_code,
    "place_of_injury": place_of_injury,
    "icd_code": icd_code,
    "cause_recode_358": cause_recode_358,
    "cause_recode_113": cause_recode_113,
    "infant_cause_recode_130": infant_cause_recode_130,
    "cause_recode_39": cause_recode_39,
    "entity_axis_conditions": entity_axis_conditions,
    "record_axis_conditions": record_axis_conditions,
    "race_recode_6": race_recode_6,
    "hispanic_origin": hispanic_origin,
    "hispanic_origin_race_recode": hispanic_origin_race_recode,
    "race_recode_40": race_recode_40,
    "decedent_occupation_code": decedent_occupation_code,
    "decedent_occupation_recode": decedent_occupation_recode,
    "decedent_industry_code": decedent_industry_code,
    "decedent_industry_recode": decedent_industry_recode,
}
