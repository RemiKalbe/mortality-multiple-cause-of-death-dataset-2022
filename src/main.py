from collections import OrderedDict
from pathlib import Path
from typing import Any, Literal, Optional
import os
import concurrent.futures
import multiprocessing
from concurrent.futures import Future
from multiprocessing.managers import DictProxy

import polars as pl
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    TaskID,
)
from rich import print
from resulty import Ok, Err, Result, propagate_result

from src.schema import (
    record_type_mapping,
    sex_mapping,
    autopsy_mapping,
    education_mapping,
    marital_status_mapping,
    manner_of_death_mapping,
    activity_code_mapping,
    race_recode_6_mapping,
    injury_at_work_mapping,
    place_of_death_mapping,
    race_recode_40_mapping,
    hispanic_origin_mapping,
    place_of_injury_mapping,
    resident_status_mapping,
    day_of_week_of_death_mapping,
    method_of_disposition_mapping,
    decedent_industry_recode_mapping,
    education_reporting_flag_mapping,
    decedent_occupation_recode_mapping,
    hispanic_origin_race_recode_mapping,
    dataset_schema,
)
from src.utils import try_parse_int, get_in_range, range_keyed_dict_has_key, maybe_del

DATASET_DIR = Path(__file__).resolve().parent.parent / "datasets"
DATASET_PATH = DATASET_DIR / "VS22MORT-1.DUSMCPUB_r20240307"
OUTPUT_PATH = DATASET_DIR / "MMCD_2022.parquet"
MAX_PARALLEL_PROCESSING = max((os.cpu_count() or 0) - 2, 1)

global progress

mapping = {
    #
    (1, 18): "reserved_positions",
    #
    # 19. Record Type:
    #       - 1 = RESIDENTS (State and County of Occurrence and Residence are the same)
    #       - 2 = NONRESIDENTS (State and/or County of Occurrence and Residence are different)
    (19, 19): "record_type",
    #
    # 20. Resident Status:
    #       - 1 = RESIDENT (State and County of Occurrence and Residence are the same)
    #       - 2 = INTRASTATE NONRESIDENT (State of Occurrence and Residence are the same, but County is different)
    #       - 3 = INTERSTATE NONRESIDENT (State of Occurrence and Residence are different, but both are in the U.S)
    #       - 4 = FOREIGN RESIDENT (State of Occurrence is one of the 50 States or the District of Columbia, but Place of Residence is outside of the U.S)
    (20, 20): "resident_status",
    #
    (21, 62): "reserved_positions",
    #
    # 63. Education:
    #       - 1 = 8th grade or less
    #       - 2 = 9 - 12th grade, no diploma
    #       - 3 = high school graduate or GED completed
    #       - 4 = some college credit, but no degree
    #       - 5 = Associate degree
    #       - 6 = Bachelor’s degree
    #       - 7 = Master’s degree
    #       - 8 = Doctorate or professional degree
    #       - 9 = Unknown
    (63, 63): "education",
    #
    # 64. Education reporting flag:
    #       - 1 = 2003 revision of education item on certificate
    #       - 2 = no education item on certificate
    (64, 64): "education_reporting_flag",
    #
    # 65-66. Month of Death:
    #       - 01 ... 12 = January ... December
    (65, 66): "month_of_death",
    #
    (67, 68): "reserved_positions",
    #
    # 69. Sex:
    #       - M = Male
    #       - F = Female
    (69, 69): "sex",
    #
    # 70-73. Detail Age:
    #       70. Age count based on:
    #           - 1 = Years
    #           - 2 = Months
    #           - 4 = Days
    #           - 5 = Hours
    #           - 6 = Minutes
    #           - 9 = Age not stated
    #       71-73. (70.) * number = age
    (70, 73): "detail_age",
    #
    # 74. Age Substitution Flag:
    #       - blank = Calculated age is not substituted for reported age
    #       - 1 = Calculated age is substituted for reported age
    (74, 74): "age_substitution_flag",
    #
    # 75-76. Age Recode 52:
    #       - 01 = Under 1 hour (includes not stated hours and minutes)
    #       - 02 = 1 - 23 hours
    #       - 03 = 1 day (includes not stated days)
    #       - 04 = 2 days
    #       - ...
    #       - 09 = 7 - 13 days (includes not stated weeks)
    #       - 10 = 14 - 20 days
    #       - 11 = 21 - 27 days
    #       - 12 = 1 month (includes not stated months)
    #       - 13 = 2 months
    #       - ...
    #       - 23 = 1 year
    #       - ...
    #       - 27 = 5 - 9 years
    #       - ...
    #       - 51 = 125 years and over
    #       - 52 = Age not stated
    (75, 76): "age_recode_52",
    #
    # 77-78. Age Recode 27:
    #       - 01 = Under 1 month (includes not stated weeks, days, hours, and minutes)
    #       - 02 = 1 month - 11 months (includes not stated months)
    #       - 03 = 1 year
    #       - ...
    #       - 07 = 5 - 9 years
    #       - ...
    #       - 26 = 100 years and over
    #       - 27 = Age not stated
    (77, 78): "age_recode_27",
    #
    # 79-80. Age Recode 12:
    #       - 01 = Under 1 year (includes not stated infant ages)
    #       - 02 = 1 - 4 years
    #       - 03 = 5 - 14 years
    #       - ...
    #       - 11 = 85 years and over
    #       - 12 = Age not stated
    (79, 80): "age_recode_12",
    #
    # 81-82. Infant Age Recode 22:
    #       - blank = Age 1 year and over or not stated
    #       - 01 = Under 1 hour (includes not stated hours and minutes)
    #       - 02 = 1 - 23 hours
    #       - 03 = 1 day (includes not stated days)
    #       - 04 = 2 days
    #       - ...
    #       - 09 = 7 - 13 days (includes not stated weeks)
    #       - 10 = 14 - 20 days
    #       - 11 = 21 - 27 days
    #       - 12 = 1 month (includes not stated months)
    #       - ...
    #       - 22 = 11 months
    (81, 82): "infant_age_recode_22",
    #
    # 83. Place of Death and Decedent’s Status:
    #       - 1 = Hospital, Clinic or Medical Center (inpatient)
    #       - 2 = Hospital, Clinic or Medical Center (outpatient or admitted to Emergency Room)
    #       - 3 = Hospital, Clinic or Medical Center (Dead on Arrival)
    #       - 4 = Decedent’s home
    #       - 5 = Hospice facility
    #       - 6 = Nursing home/long term care
    #       - 7 = Other
    #       - 9 = Place of death unknown
    (83, 83): "place_of_death_and_decedents_status",
    #
    # 84. Marital Status:
    #       - S = Never married, single
    #       - M = Married
    #       - W = Widowed
    #       - D = Divorced
    #       - U = Marital Status unknown
    (84, 84): "marital_status",
    #
    # 85. Day of Week of Death:
    #       - 1 = Sunday
    #       - 2 = Monday
    #       - 3 = Tuesday
    #       - 4 = Wednesday
    #       - 5 = Thursday
    #       - 6 = Friday
    #       - 7 = Saturday
    #       - 9 = Unknown
    (85, 85): "day_of_week_of_death",
    #
    (86, 101): "reserved_positions",
    #
    # 102-105. Current Data Year:
    #       - 2022 = 2022
    (102, 105): "current_data_year",
    #
    # 106. Injury at Work:
    #       - Y = Yes
    #       - N = No
    #       - U = Unknown
    (106, 106): "injury_at_work",
    #
    # 107. Manner of Death:
    #       - 1 = Accident
    #       - 2 = Sucide
    #       - 3 = Homicide
    #       - 4 = Pending investigation
    #       - 5 = Could not determine
    #       - 6 = Self-Inflicted
    #       - 7 = Natural
    #       - blank = Not specified
    (107, 107): "manner_of_death",
    #
    # 108. Method of Disposition:
    #       - B = Burial
    #       - C = Cremation
    #       - D = Donation
    #       - E = Entombment
    #       - O = Other
    #       - R = Removal from jurisdiction
    #       - U = Unknown
    (108, 108): "method_of_disposition",
    #
    # 109. Autopsy:
    #       - Y = Yes
    #       - N = No
    #       - U = Unknown
    (109, 109): "autopsy",
    #
    (110, 143): "reserved_positions",
    #
    # 144. Activity Code:
    #       - 0 = While engaged in sports activity
    #       - 1 = While engaged in leisure activity
    #       - 2 = While working for income
    #       - 3 = While engaged in other types of work
    #       - 4 = While resting, sleeping, eating (vital activities)
    #       - 8 = While engaged in other specified activities
    #       - 9 = During unspecified activity
    #       - blank = Not specified
    (144, 144): "activity_code",
    #
    # 145. Place of Injury:
    #       - 0 = Home
    #       - 1 = Residential institution
    #       - 2 = School, other institution and public administrative area
    #       - 3 = Sports and athletics area
    #       - 4 = Street and highway
    #       - 5 = Trade and service area
    #       - 6 = Industrial and construction area
    #       - 7 = Farm
    #       - 8 = Other specified places
    #       - 9 = Unspecified place
    #       - blank = Causes other than W00-Y34, except Y06.- and Y07.-
    (145, 145): "place_of_injury",
    #
    # 146-149. ICD Code (10th Revision) for the Underlying Cause of Death:
    #       - See https://www.cdc.gov/nchs/nvss/manuals/2022/2e_volume1_2022.htm
    #       - See https://www.cdc.gov/nchs/nvss/manuals/2b-appendix-2022.htm
    (146, 149): "icd_code",
    #
    # 150-152. 358 Cause-Recode:
    #       - 001 https://resdac.org/sites/datadocumentation.resdac.org/files/358%20ICD-10%20Cause%20of%20Death%20Recodes%20Code%20Table%20%28MBSF-NDI%29.txt
    #       - ...
    #       - 456
    (150, 152): "cause_recode_358",
    #
    (153, 153): "reserved_positions",
    #
    # 154-156. 113 Cause Recode:
    #       - 001 https://resdac.org/sites/datadocumentation.resdac.org/files/113%20ICD-10%20Cause%20of%20Death%20Recodes%20Code%20Table%20%28MBSF-NDI%29.txt
    #       - ...
    #       - 135
    (154, 156): "cause_recode_113",
    #
    # 157-159. 130 Infant Cause Recode:
    #       - 001 https://resdac.org/sites/datadocumentation.resdac.org/files/130%20ICD-10%20Cause%20of%20Death%20Recodes%20Code%20Table%20%28MBSF-NDI%29.txt
    #       - ...
    #       - 158
    (157, 159): "infant_cause_recode_130",
    #
    # 160-161. 39 Cause Recode:
    #       - 001 https://hhdw.org/report/contentfile/html/NCHS39.htm
    #       - ...
    #       - 42
    (160, 161): "cause_recode_39",
    #
    (162, 162): "reserved_positions",
    #
    # 163-164. Number of Entity Axis Conditions:
    #       - 00 ... 20 = Code range
    (163, 164): "number_of_entity_axis_conditions",
    #
    # 165-304. List of Entity Axis Conditions:
    #       - 165-171. 1st Condition:
    #           - 165. Part/line number on certificate:
    #               - 1 = Part I, line 1 (a)
    #               - 2 = Part I, line 2 (b)
    #               - 3 = Part I, line 3 (c)
    #               - 4 = Part I, line 4 (d)
    #               - 5 = Part I, line 5 (e)
    #               - 6 = Part II
    #           - 166.  Sequence of condition within part/line
    #               - 1 ... 7 = Code range
    #           - 167-171. Condition code
    (165, 171): "entity_axis_condition_1",
    (172, 178): "entity_axis_condition_2",
    (179, 185): "entity_axis_condition_3",
    (186, 192): "entity_axis_condition_4",
    (193, 199): "entity_axis_condition_5",
    (200, 206): "entity_axis_condition_6",
    (207, 213): "entity_axis_condition_7",
    (214, 220): "entity_axis_condition_8",
    (221, 227): "entity_axis_condition_9",
    (228, 234): "entity_axis_condition_10",
    (235, 241): "entity_axis_condition_11",
    (242, 248): "entity_axis_condition_12",
    (249, 255): "entity_axis_condition_13",
    (256, 262): "entity_axis_condition_14",
    (263, 269): "entity_axis_condition_15",
    (270, 276): "entity_axis_condition_16",
    (277, 283): "entity_axis_condition_17",
    (284, 290): "entity_axis_condition_18",
    (291, 297): "entity_axis_condition_19",
    (298, 304): "entity_axis_condition_20",
    #
    (305, 340): "reserved_positions",
    #
    # 341-342. Number of Record-Axis Conditions:
    #       - 00 ... 20 = Code range
    (341, 342): "number_of_record_axis_conditions",
    #
    (343, 343): "reserved_positions",
    #
    # 344-443. List of Record-Axis Conditions:
    #       - 344-348. 1st Condition:
    #           - 344-348: Condition code
    (344, 348): "record_axis_condition_1",
    (349, 353): "record_axis_condition_2",
    (354, 358): "record_axis_condition_3",
    (359, 363): "record_axis_condition_4",
    (364, 368): "record_axis_condition_5",
    (369, 373): "record_axis_condition_6",
    (374, 378): "record_axis_condition_7",
    (379, 383): "record_axis_condition_8",
    (384, 388): "record_axis_condition_9",
    (389, 393): "record_axis_condition_10",
    (394, 398): "record_axis_condition_11",
    (399, 403): "record_axis_condition_12",
    (404, 408): "record_axis_condition_13",
    (409, 413): "record_axis_condition_14",
    (414, 418): "record_axis_condition_15",
    (419, 423): "record_axis_condition_16",
    (424, 428): "record_axis_condition_17",
    (429, 433): "record_axis_condition_18",
    (434, 438): "record_axis_condition_19",
    (439, 443): "record_axis_condition_20",
    #
    (444, 447): "reserved_positions",
    #
    # 448. Race Imputation Flag:
    #      - blank = Race is not imputed
    #      - 1 = Unknown race is imputed
    #      - 2 = All other races is imputed
    (448, 448): "race_imputation_flag",
    #
    (449, 449): "reserved_positions",
    #
    # 450. Race Recode 6
    #      - 1 = White (only)
    #      - 2 = Black (only)
    #      - 3 = American Indian and Alaska Native (only)
    #      - 4 = Asian (only)
    #      - 5 = Native Hawaiian and Other Pacific Islander (only)
    #      - 6 = More than one race
    (450, 450): "race_recode_6",
    #
    (451, 483): "reserved_positions",
    #
    # 484-486. Hispanic Origin:
    #      - 100-199 = Non-Hispanic
    #      - 200-209 = Spaniard
    #      - 210-219 = Mexican
    #      - 220-230 = Central American
    #      - 231-249 = South American
    #      - 250-259 = Latin American
    #      - 260-269 = Puerto Rican
    #      - 270-274 = Cuban
    #      - 275-279 = Dominican
    #      - 280-299 = Other Hispanic
    #      - 996-999 = Unknown
    (484, 486): "hispanic_origin",
    #
    # 487-488. Hispanic Origin/Race Recode:
    #      - 01 = Mexican
    #      - 02 = Puerto Rican
    #      - 03 = Cuban
    #      - 04 = Dominican
    #      - 05 = Central American
    #      - 06 = South American
    #      - 07 = Other or Unknown Hispanic
    #      - 08 = Non-Hispanic White (only)
    #      - 09 = Non-Hispanic Black (only)
    #      - 10 = Non-Hispanic American Indian and Alaska Native (only)
    #      - 11 = Non-Hispanic Asian (only)
    #      - 12 = Non-Hispanic Native Hawaiian and Other Pacific Islander (only)
    #      - 13 = Non-Hispanic more than one race
    #      - 14 = Hispanic origin unknown or not stated
    (487, 488): "hispanic_origin_race_recode",
    #
    # 489-490. Race Recode 40:
    #      - 01 = White
    #      - 02 = Black
    #      - 03 = American Indian and Alaskan Native (AIAN)
    #      - 04 = Asian Indian
    #      - 05 = Chinese
    #      - 06 = Filipino
    #      - 07 = Japanese
    #      - 08 = Korean
    #      - 09 = Vietnamese
    #      - 10 = Other or Multiple Asian
    #      - 11 = Hawaiian
    #      - 12 = Guamanian
    #      - 13 = Samoan
    #      - 14 = Other or Multiple Pacific Islander
    #      - 15 = Black and White
    #      - 16 = Black and AIAN
    #      - 17 = Black and Asian
    #      - 18 = Black and Native Hawaiian or Other Pacific Islander (NHOPI)
    #      - 19 = AIAN and White
    #      - 20 = AIAN and Asian
    #      - 21 = AIAN and NHOPI
    #      - 22 = Asian and White
    #      - 23 = Asian and NHOPI
    #      - 24 = NHOPI and White
    #      - 25 = Black, AIAN and White
    #      - 26 = Black, AIAN and Asian
    #      - 27 = Black, AIAN and NHOPI
    #      - 28 = Black, Asian and White
    #      - 29 = Black, Asian and NHOPI
    #      - 30 = Black, NHOPI and White
    #      - 31 = AIAN, Asian and White
    #      - 32 = AIAN, NHOPI and White
    #      - 33 = AIAN, Asian and NHOPI
    #      - 34 = Asian, NHOPI and White
    #      - 35 = Black, AIAN, Asian and White
    #      - 36 = Black, AIAN, Asian and NHOPI
    #      - 37 = Black, AIAN, NHOPI and White
    #      - 38 = Black, Asian, NHOPI and White
    #      - 39 = AIAN, Asian, NHOPI and White
    #      - 40 = Black, AIAN, Asian, NHOPI and White
    (489, 490): "race_recode_40",
    #
    (491, 805): "reserved_positions",
    #
    # 806-817. Decedent’S Usual Occupation And Industry:
    #      - 806-809. Occupation 4-digit code
    #          - See https://www.cdc.gov/nchs/data/dvs/Industry-and-Occupation-data-mortality-2020.pdf
    #      - 810-811. Occupation recode
    #          - 01 = MANAGEMENT OCCUPATIONS
    #          - 02 = BUSINESS AND FINANCIAL OPERATIONS OCCUPATIONS
    #          - 03 = COMPUTER AND MATHEMATICAL OCCUPATIONS
    #          - 04 = ARCHITECTURE AND ENGINEERING OCCUPATIONS
    #          - 05 = LIFE, PHYSICAL, AND SOCIAL SCIENCE OCCUPATIONS
    #          - 06 = COMMUNITY AND SOCIAL SERVICES OCCUPATIONS
    #          - 07 = LEGAL OCCUPATIONS
    #          - 08 = EDUCATION, TRAINING, AND LIBRARY OCCUPATIONS
    #          - 09 = ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA OCCUPATIONS
    #          - 10 = HEALTHCARE PRACTITIONERS AND TECHNICAL OCCUPATIONS
    #          - 11 = HEALTHCARE SUPPORT OCCUPATIONS
    #          - 12 = PROTECTIVE SERVICE OCCUPATIONS
    #          - 13 = FOOD PREPARATION AND SERVING RELATED OCCUPATIONS
    #          - 14 = BUILDING AND GROUNDS CLEANING AND MAINTENANCE OCCUPATIONS
    #          - 15 = PERSONAL CARE AND SERVICE OCCUPATIONS
    #          - 16 = SALES AND RELATED OCCUPATIONS
    #          - 17 = OFFICE AND ADMINISTRATIVE SUPPORT OCCUPATIONS
    #          - 18 = FARMING, FISHING, AND FORESTRY OCCUPATIONS
    #          - 19 = CONSTRUCTION AND EXTRACTION OCCUPATIONS
    #          - 20 = INSTALLATION, MAINTENANCE, AND REPAIR OCCUPATIONS
    #          - 21 = PRODUCTION OCCUPATIONS
    #          - 22 = TRANSPORTATION AND MATERIAL MOVING OCCUPATIONS
    #          - 24 = MILITARY SPECIFIC OCCUPATIONS
    #          - 25 = OTHER—MISC (EXC HOUSEWIFE)
    #          - 26 = HOUSEWIFE
    #      - 812-815. Industry 4-digit code
    #          - See https://www.cdc.gov/niosh/topics/coding/more.html
    #      - 816-817. Industry recode
    #          - 01 = AGRICULTURE, FORESTRY, FISHING, AND HUNTING IND
    #          - 02 = MINING IND
    #          - 03 = UTILITIES IND
    #          - 04 = CONSTRUCTION IND
    #          - 05 = MANUFACTURING IND
    #          - 06 = WHOLESALE TRADE IND
    #          - 07 = RETAIL TRADE IND
    #          - 08 = TRANSPORTATION AND WAREHOUSING IND
    #          - 09 = INFORMATION IND
    #          - 10 = FINANCE AND INSURANCE IND
    #          - 11 = REAL ESTATE AND RENTAL AND LEASING IND
    #          - 12 = PROFESSIONAL, SCIENTIFIC, AND TECHNICAL SERVICES IND
    #          - 13 = MANAGEMENT OF COMPANIES AND ENTERPRISES IND
    #          - 14 = ADMINISTRATIVE AND SUPPORT AND WASTE MANAGEMENT AND REMEDIATION SERVICES IND
    #          - 15 = EDUCATIONAL SERVICES IND
    #          - 16 = HEALTH CARE AND SOCIAL ASSISTANCE IND
    #          - 17 = ARTS, ENTERTAINMENT, AND RECREATION IND
    #          - 18 = ACCOMMODATION AND FOOD SERVICES IND
    #          - 19 = OTHER SERVICES (EXCEPT PUBLIC ADMINISTRATION) IND
    #          - 20 = PUBLIC ADMINISTRATION IND
    #          - 22 = MILITARY
    #          - 23 = OTHER-MISC, MISSING
    (806, 809): "decedents_usual_occindu_occupation_4_digit_code",
    (810, 811): "decedents_usual_occindu_occupation_recode",
    (812, 815): "decedents_usual_occindu_industry_4_digit_code",
    (816, 817): "decedents_usual_occindu_industry_recode",
}


@propagate_result
def process_record_type(
    value: str,
) -> Result[tuple[Literal["record_type"], Optional[str]], str]:
    if value == "":
        return Ok(("record_type", None))

    v = try_parse_int(value).up()
    mapped = record_type_mapping.get(v)
    if mapped:
        return Ok(("record_type", mapped))
    else:
        return Err(f"Invalid value for record_type: {v}")


@propagate_result
def process_resident_status(
    value: str,
) -> Result[tuple[Literal["resident_status"], Optional[str]], str]:
    if value == "":
        return Ok(("resident_status", None))

    v = try_parse_int(value).up()
    mapped = resident_status_mapping.get(v)
    if mapped:
        return Ok(("resident_status", mapped))
    else:
        return Err(f"Invalid value for resident_status: {v}")


@propagate_result
def process_education(
    value: str,
) -> Result[tuple[Literal["education"], Optional[str]], str]:
    if value == "":
        return Ok(("education", None))

    v = try_parse_int(value).up()
    mapped = education_mapping.get(v)
    if mapped or v in education_mapping.keys():
        return Ok(("education", mapped))
    else:
        return Err(f"Invalid value for education: {v}")


@propagate_result
def process_education_reporting_flag(
    value: str,
) -> Result[tuple[Literal["education_reporting_flag"], Optional[str]], str]:
    if value == "":
        return Ok(("education_reporting_flag", None))

    v = try_parse_int(value).up()
    mapped = education_reporting_flag_mapping.get(v)
    if mapped:
        return Ok(("education_reporting_flag", mapped))
    else:
        return Err(f"Invalid value for education_reporting_flag: {v}")


@propagate_result
def process_month_of_death(
    value: str,
) -> Result[tuple[Literal["month_of_death"], Optional[int]], str]:
    if value == "":
        return Ok(("month_of_death", None))

    v = try_parse_int(value).up()
    if 1 <= v <= 12:
        return Ok(("month_of_death", v))
    else:
        return Err(f"Invalid value for month_of_death: {v}")


@propagate_result
def process_sex(
    value: str,
) -> Result[tuple[Literal["sex"], Optional[str]], str]:
    if value == "":
        return Ok(("sex", None))

    mapped = sex_mapping.get(value)
    if mapped:
        return Ok(("sex", mapped))
    else:
        return Err(f"Invalid value for sex: {value}")


MINUTES_AS_MS = 60_000
HOURS_AS_MS = 60 * MINUTES_AS_MS
DAYS_AS_MS = 24 * HOURS_AS_MS
MONTHS_AS_MS = 30 * DAYS_AS_MS
YEARS_AS_MS = 365 * DAYS_AS_MS


@propagate_result
def process_detail_age(
    value: str,
) -> Result[tuple[Literal["detail_age"], Optional[int]], str]:
    if value == "":
        return Ok(("detail_age", None))

    based_on, count = (
        try_parse_int(value[:1]).up(),
        try_parse_int(value[1:]).up(),
    )

    if 1 <= based_on <= 6:
        mult: float = {
            1: YEARS_AS_MS,
            2: MONTHS_AS_MS,
            4: DAYS_AS_MS,
            5: HOURS_AS_MS,
            6: MINUTES_AS_MS,
        }[based_on]

        return Ok(("detail_age", int(count * mult)))
    else:
        return Ok(("detail_age", None))


@propagate_result
def process_age_substitution_flag(
    value: str,
) -> Result[tuple[Literal["age_substitution_flag"], bool], str]:
    if value == "":
        return Ok(("age_substitution_flag", False))

    v = try_parse_int(value).up()

    if v == 1:
        return Ok(("age_substitution_flag", True))
    else:
        return Err(f"Invalid value for age_substitution_flag: {value}")


@propagate_result
def process_age_recode_52(
    value: str,
) -> Result[tuple[Literal["age_recode_52"], Optional[tuple[int, int]]], str]:
    if value == "":
        return Ok(("age_recode_52", None))

    v = try_parse_int(value).up()
    if 1 <= v <= 51:
        if v == 1:
            low = 0
            upper = HOURS_AS_MS
        elif v == 2:
            low = HOURS_AS_MS
            upper = 23 * HOURS_AS_MS
        elif 3 <= v <= 9:
            i = v - 2
            low = i * DAYS_AS_MS
            upper = i * DAYS_AS_MS
        elif 10 <= v <= 11:
            i = v - 10
            low = (14 + 7 * i) * DAYS_AS_MS
            upper = (13 + 7 * (i + 1)) * DAYS_AS_MS
        elif 12 <= v <= 22:
            i = v - 11
            low = i * MONTHS_AS_MS
            upper = i * MONTHS_AS_MS
        elif 23 <= v <= 26:
            i = v - 22
            low = i * YEARS_AS_MS
            upper = i * YEARS_AS_MS
        elif 27 <= v <= 50:
            i = v - 27
            low = (5 + 5 * i) * YEARS_AS_MS
            upper = (4 + 5 * (i + 1)) * YEARS_AS_MS
        elif v == 51:
            low = 125 * YEARS_AS_MS
            upper = 999 * YEARS_AS_MS
        else:
            return Err(f"Invalid value for age_recode_52: {v}")
        return Ok(("age_recode_52", (low, upper)))
    elif v == 52:
        return Ok(("age_recode_52", None))
    else:
        return Err(f"Invalid value for age_recode_52: {v}")


@propagate_result
def process_age_recode_27(
    value: str,
) -> Result[tuple[Literal["age_recode_27"], Optional[tuple[int, int]]], str]:
    if value == "":
        return Ok(("age_recode_27", None))

    v = try_parse_int(value).up()
    if 1 <= v <= 26:
        if v == 1:
            low = 0
            upper = 1 * MONTHS_AS_MS
        elif v == 2:
            low = 1 * MONTHS_AS_MS
            upper = 11 * MONTHS_AS_MS
        elif 3 <= v <= 6:
            i = v - 2
            low = i * YEARS_AS_MS
            upper = i * YEARS_AS_MS
        elif 7 <= v <= 25:
            i = v - 7
            low = (5 + 5 * i) * YEARS_AS_MS
            upper = (4 + 5 * (i + 1)) * YEARS_AS_MS
        elif v == 26:
            low = 100 * YEARS_AS_MS
            upper = 999 * YEARS_AS_MS
        else:
            return Err(f"Invalid value for age_recode_27: {v}")
        return Ok(("age_recode_27", (low, upper)))
    elif v == 27:
        return Ok(("age_recode_27", None))
    else:
        return Err(f"Invalid value for age_recode_27: {v}")


@propagate_result
def process_age_recode_12(
    value: str,
) -> Result[tuple[Literal["age_recode_12"], Optional[tuple[int, int]]], str]:
    if value == "":
        return Ok(("age_recode_12", None))

    v = try_parse_int(value).up()
    if 1 <= v <= 11:
        if v == 1:
            low = 0
            upper = 11 * MONTHS_AS_MS
        elif v == 2:
            low = 1 * YEARS_AS_MS
            upper = 4 * YEARS_AS_MS
        elif 3 <= v <= 10:
            i = v - 3
            low = (5 + 10 * i) * YEARS_AS_MS
            upper = (4 + 10 * (i + 1)) * YEARS_AS_MS
        elif v == 11:
            low = 85 * YEARS_AS_MS
            upper = 999 * YEARS_AS_MS
        else:
            return Err(f"Invalid value for age_recode_12: {v}")
        return Ok(("age_recode_12", (low, upper)))
    elif v == 12:
        return Ok(("age_recode_12", None))
    else:
        return Err(f"Invalid value for age_recode_12: {v}")


@propagate_result
def process_infant_age_recode_22(
    value: str,
) -> Result[tuple[Literal["infant_age_recode_22"], Optional[tuple[int, int]]], str]:
    if value == "":
        return Ok(("infant_age_recode_22", None))

    v = try_parse_int(value).up()
    if 1 <= v <= 22:
        if v == 1:
            low = 0
            upper = 59 * MINUTES_AS_MS
        elif v == 2:
            low = 1 * HOURS_AS_MS
            upper = 23 * HOURS_AS_MS
        elif 3 <= v <= 8:
            i = v - 2
            low = i * DAYS_AS_MS
            upper = i * DAYS_AS_MS
        elif 9 <= v <= 11:
            i = v - 9
            low = (7 + 7 * i) * DAYS_AS_MS
            upper = (6 + 7 * (i + 1)) * DAYS_AS_MS
        elif 12 <= v <= 22:
            i = v - 11
            low = i * MONTHS_AS_MS
            upper = i * MONTHS_AS_MS
        else:
            return Err(f"Invalid value for infant_age_recode_22: {v}")
        return Ok(("infant_age_recode_22", (low, upper)))
    else:
        return Err(f"Invalid value for infant_age_recode_22: {v}")


@propagate_result
def process_place_of_death(
    value: str,
) -> Result[tuple[Literal["place_of_death"], Optional[str]], str]:
    if value == "":
        return Ok(("place_of_death", None))

    v = try_parse_int(value).up()

    mapped = place_of_death_mapping.get(v)
    if mapped or v in place_of_death_mapping.keys():
        return Ok(("place_of_death", mapped))
    else:
        return Err(f"Invalid value for place_of_death_and_decedents_status: {value}")


@propagate_result
def process_marital_status(
    value: str,
) -> Result[tuple[Literal["marital_status"], Optional[str]], str]:
    if value == "":
        return Ok(("marital_status", None))

    mapped = marital_status_mapping.get(value)
    if mapped or value in marital_status_mapping.keys():
        return Ok(("marital_status", mapped))
    else:
        return Err(f"Invalid value for marital_status: {value}")


@propagate_result
def process_day_of_week_of_death(
    value: str,
) -> Result[tuple[Literal["day_of_week_of_death"], Optional[str]], str]:
    if value == "":
        return Ok(("day_of_week_of_death", None))

    v = try_parse_int(value).up()

    mapped = day_of_week_of_death_mapping.get(v)
    if mapped or v in day_of_week_of_death_mapping.keys():
        return Ok(("day_of_week_of_death", mapped))
    else:
        return Err(f"Invalid value for day_of_week_of_death: {value}")


@propagate_result
def process_injury_at_work(
    value: str,
) -> Result[tuple[Literal["injury_at_work"], Optional[str]], str]:
    if value == "":
        return Ok(("injury_at_work", None))

    mapped = injury_at_work_mapping.get(value)
    if mapped or value in injury_at_work_mapping.keys():
        return Ok(("injury_at_work", mapped))
    else:
        return Err(f"Invalid value for injury_at_work: {value}")


@propagate_result
def process_manner_of_death(
    value: str,
) -> Result[tuple[Literal["manner_of_death"], Optional[str]], str]:
    if value == "":
        return Ok(("manner_of_death", None))

    v = try_parse_int(value).up()

    mapped = manner_of_death_mapping.get(v)
    if mapped:
        return Ok(("manner_of_death", mapped))
    else:
        return Err(f"Invalid value for manner_of_death: {value}")


@propagate_result
def process_method_of_disposition(
    value: str,
) -> Result[tuple[Literal["method_of_disposition"], Optional[str]], str]:
    if value == "":
        return Ok(("method_of_disposition", None))

    mapped = method_of_disposition_mapping.get(value)
    if mapped or value in method_of_disposition_mapping.keys():
        return Ok(("method_of_disposition", mapped))
    else:
        return Err(f"Invalid value for method_of_disposition: {value}")


@propagate_result
def process_autopsy(
    value: str,
) -> Result[tuple[Literal["autopsy"], Optional[str]], str]:
    if value == "":
        return Ok(("autopsy", None))

    mapped = autopsy_mapping.get(value)
    if mapped or value in autopsy_mapping.keys():
        return Ok(("autopsy", mapped))
    else:
        return Err(f"Invalid value for autopsy: {value}")


@propagate_result
def process_activity_code(
    value: str,
) -> Result[tuple[Literal["activity_code"], Optional[str]], str]:
    if value == "":
        return Ok(("activity_code", None))

    v = try_parse_int(value).up()

    mapped = activity_code_mapping.get(v)
    if mapped or v in activity_code_mapping.keys():
        return Ok(("activity_code", mapped))
    else:
        return Err(f"Invalid value for activity_code: {value}")


@propagate_result
def process_place_of_injury(
    value: str,
) -> Result[tuple[Literal["place_of_injury"], Optional[str]], str]:
    if value == "":
        return Ok(("place_of_injury", None))

    v = try_parse_int(value).up()

    mapped = place_of_injury_mapping.get(v)
    if mapped or v in place_of_injury_mapping.keys():
        return Ok(("place_of_injury", mapped))
    else:
        return Err(f"Invalid value for place_of_injury: {value}")


@propagate_result
def process_icd_code(
    value: str,
) -> Result[tuple[Literal["icd_code"], Optional[str]], str]:
    if value == "":
        return Ok(("icd_code", None))

    return Ok(("icd_code", value))


@propagate_result
def process_cause_recode_358(
    value: str,
) -> Result[tuple[Literal["cause_recode_358"], Optional[str]], str]:
    if value == "":
        return Ok(("cause_recode_358", None))

    return Ok(("cause_recode_358", value))


@propagate_result
def process_cause_recode_113(
    value: str,
) -> Result[tuple[Literal["cause_recode_113"], Optional[str]], str]:
    if value == "":
        return Ok(("cause_recode_113", None))

    return Ok(("cause_recode_113", value))


@propagate_result
def process_infant_cause_recode_130(
    value: str,
) -> Result[tuple[Literal["infant_cause_recode_130"], Optional[str]], str]:
    if value == "":
        return Ok(("infant_cause_recode_130", None))

    return Ok(("infant_cause_recode_130", value))


@propagate_result
def process_cause_recode_39(
    value: str,
) -> Result[tuple[Literal["cause_recode_39"], Optional[int]], str]:
    if value == "":
        return Ok(("cause_recode_39", None))

    v = try_parse_int(value).up()
    if 1 <= v <= 42:
        return Ok(("cause_recode_39", v))
    else:
        return Ok(("cause_recode_39", None))


@propagate_result
def process_number_of_entity_axis_conditions(
    value: str,
) -> Result[tuple[Literal["number_of_entity_axis_conditions"], Optional[int]], str]:
    if value == "":
        return Ok(("number_of_entity_axis_conditions", None))

    v = try_parse_int(value).up()
    if 0 <= v <= 20:
        return Ok(("number_of_entity_axis_conditions", v))
    else:
        return Err(f"Invalid value for number_of_entity_axis_conditions: {v}")


@propagate_result
def process_entity_axis_condition(
    n: int,
    value: str,
) -> Result[tuple[str, Optional[dict[str, str | int]]], str]:
    if value == "":
        return Ok((f"entity_axis_condition_{n}", None))

    part_line = try_parse_int(value[0]).up()
    part = "PART_I" if part_line <= 5 else "PART_II"

    certificate_line = try_parse_int(value[1]).up()

    condition_code = value[2:]

    return Ok(
        (
            f"entity_axis_condition_{n}",
            {
                "certificate_part": part,
                "certificate_line": certificate_line,
                "condition": condition_code,
            },
        )
    )


@propagate_result
def process_number_of_record_axis_conditions(
    value: str,
) -> Result[tuple[Literal["number_of_record_axis_conditions"], Optional[int]], str]:
    if value == "":
        return Ok(("number_of_record_axis_conditions", None))

    v = try_parse_int(value).up()
    if 0 <= v <= 20:
        return Ok(("number_of_record_axis_conditions", v))
    else:
        return Err(f"Invalid value for number_of_record_axis_conditions: {v}")


@propagate_result
def process_record_axis_condition(
    n: int,
    value: str,
) -> Result[tuple[str, Optional[str]], str]:
    if value == "":
        return Ok((f"record_axis_condition_{n}", None))

    return Ok((f"record_axis_condition_{n}", value))


@propagate_result
def process_race_imputation_flag(
    value: str,
) -> Result[tuple[Literal["race_imputation_flag"], bool], str]:
    if value == "":
        return Ok(("race_imputation_flag", False))

    v = try_parse_int(value).up()
    if v == 1 or v == 2:
        return Ok(("race_imputation_flag", True))
    else:
        return Err(f"Invalid value for race_imputation_flag: {v}")


@propagate_result
def process_race_recode_6(
    value: str,
) -> Result[tuple[Literal["race_recode_6"], Optional[str]], str]:
    if value == "":
        return Ok(("race_recode_6", None))

    v = try_parse_int(value).up()
    mapped = race_recode_6_mapping.get(v)
    if mapped:
        return Ok(("race_recode_6", mapped))
    else:
        return Err(f"Invalid value for race_recode_6: {v}")


@propagate_result
def process_hispanic_origin(
    value: str,
) -> Result[tuple[Literal["hispanic_origin"], Optional[str]], str]:
    if value == "":
        return Ok(("hispanic_origin", None))

    v = try_parse_int(value).up()
    mapped = get_in_range(hispanic_origin_mapping, v)
    if mapped or range_keyed_dict_has_key(hispanic_origin_mapping, v):
        return Ok(("hispanic_origin", mapped))
    else:
        return Err(f"Invalid value for hispanic_origin: {v}")


@propagate_result
def process_hispanic_origin_race_recode(
    value: str,
) -> Result[tuple[Literal["hispanic_origin_race_recode"], Optional[str]], str]:
    if value == "":
        return Ok(("hispanic_origin_race_recode", None))

    v = try_parse_int(value).up()
    mapped = hispanic_origin_race_recode_mapping.get(v)

    if mapped or v in hispanic_origin_race_recode_mapping:
        return Ok(("hispanic_origin_race_recode", mapped))
    else:
        return Err(f"Invalid value for hispanic_origin_race_recode: {v}")


@propagate_result
def process_race_recode_40(
    value: str,
) -> Result[tuple[Literal["race_recode_40"], Optional[list[str]]], str]:
    if value == "":
        return Ok(("race_recode_40", None))

    v = try_parse_int(value).up()
    mapped = race_recode_40_mapping.get(v)

    if mapped:
        if v <= 14:
            return Ok(("race_recode_40", [mapped]))
        else:
            parts = mapped.split("_")
            for part in parts:
                if part == "ASIAN":
                    part = "OTHER_ASIAN"
                elif part == "NHOPI":
                    part = "OTHER_PACIFIC_ISLANDER"
            return Ok(("race_recode_40", parts))
    else:
        return Err(f"Invalid value for race_recode_40: {v}")


@propagate_result
def process_decedent_occupation_code(
    value: str,
) -> Result[tuple[Literal["decedent_occupation_code"], Optional[str]], str]:
    if value == "":
        return Ok(("decedent_occupation_code", None))

    return Ok(("decedent_occupation_code", value))


@propagate_result
def process_decedent_occupation_recode(
    value: str,
) -> Result[tuple[Literal["decedent_occupation_recode"], Optional[str]], str]:
    if value == "":
        return Ok(("decedent_occupation_recode", None))

    v = try_parse_int(value).up()
    mapped = decedent_occupation_recode_mapping.get(v)
    if mapped or v in decedent_occupation_recode_mapping.keys():
        return Ok(("decedent_occupation_recode", mapped))
    else:
        return Err(f"Invalid value for decedent_occupation_recode: {v}")


@propagate_result
def process_decedent_industry_code(
    value: str,
) -> Result[tuple[Literal["decedent_industry_code"], Optional[str]], str]:
    if value == "":
        return Ok(("decedent_industry_code", None))

    return Ok(("decedent_industry_code", value))


@propagate_result
def process_decedent_industry_recode(
    value: str,
) -> Result[tuple[Literal["decedent_industry_recode"], Optional[str]], str]:
    if value == "":
        return Ok(("decedent_industry_recode", None))

    v = try_parse_int(value).up()
    mapped = decedent_industry_recode_mapping.get(v)
    if mapped or v in decedent_industry_recode_mapping.keys():
        return Ok(("decedent_industry_recode", mapped))
    else:
        return Err(f"Invalid value for decedent_industry_recode: {v}")


@propagate_result
def process_line(line: str) -> Result[OrderedDict[str, Any], str]:
    d = OrderedDict()
    for (start, end), field in mapping.items():
        value = line[start - 1 : end].strip()
        if field == "reserved_positions":
            continue
        elif field == "record_type":
            k, v = process_record_type(value).up()
            d[k] = v
        elif field == "resident_status":
            k, v = process_resident_status(value).up()
            d[k] = v
        elif field == "education":
            k, v = process_education(value).up()
            d[k] = v
        elif field == "education_reporting_flag":
            k, v = process_education_reporting_flag(value).up()
        elif field == "month_of_death":
            k, v = process_month_of_death(value).up()
            d[k] = v
        elif field == "sex":
            k, v = process_sex(value).up()
            d[k] = v
        elif field == "detail_age":
            k, v = process_detail_age(value).up()
            d[k] = v
        elif field == "age_substitution_flag":
            k, v = process_age_substitution_flag(value).up()
            d[k] = v
        elif field == "age_recode_52":
            k, v = process_age_recode_52(value).up()
            d[k] = v
        elif field == "age_recode_27":
            k, v = process_age_recode_27(value).up()
            d[k] = v
        elif field == "age_recode_12":
            k, v = process_age_recode_12(value).up()
            d[k] = v
        elif field == "infant_age_recode_22":
            k, v = process_infant_age_recode_22(value).up()
            d[k] = v
        elif field == "place_of_death_and_decedents_status":
            k, v = process_place_of_death(value).up()
            d[k] = v
        elif field == "marital_status":
            k, v = process_marital_status(value).up()
            d[k] = v
        elif field == "day_of_week_of_death":
            k, v = process_day_of_week_of_death(value).up()
            d[k] = v
        elif field == "injury_at_work":
            k, v = process_injury_at_work(value).up()
            d[k] = v
        elif field == "manner_of_death":
            k, v = process_manner_of_death(value).up()
            d[k] = v
        elif field == "method_of_disposition":
            k, v = process_method_of_disposition(value).up()
            d[k] = v
        elif field == "autopsy":
            k, v = process_autopsy(value).up()
            d[k] = v
        elif field == "activity_code":
            k, v = process_activity_code(value).up()
            d[k] = v
        elif field == "place_of_injury":
            k, v = process_place_of_injury(value).up()
            d[k] = v
        elif field == "icd_code":
            k, v = process_icd_code(value).up()
            d[k] = v
        elif field == "cause_recode_358":
            k, v = process_cause_recode_358(value).up()
            d[k] = v
        elif field == "cause_recode_113":
            k, v = process_cause_recode_113(value).up()
            d[k] = v
        elif field == "infant_cause_recode_130":
            k, v = process_infant_cause_recode_130(value).up()
            d[k] = v
        elif field == "cause_recode_39":
            k, v = process_cause_recode_39(value).up()
            d[k] = v
        elif field == "number_of_entity_axis_conditions":
            k, v = process_number_of_entity_axis_conditions(value).up()
            d[k] = v
        elif field.startswith("entity_axis_condition_"):
            n = int(field.split("_")[-1])
            k, v = process_entity_axis_condition(n, value).up()
            d[k] = v
        elif field == "number_of_record_axis_conditions":
            k, v = process_number_of_record_axis_conditions(value).up()
            d[k] = v
        elif field.startswith("record_axis_condition_"):
            n = int(field.split("_")[-1])
            k, v = process_record_axis_condition(n, value).up()
            d[k] = v
        elif field == "race_imputation_flag":
            k, v = process_race_imputation_flag(value).up()
            d[k] = v
        elif field == "race_recode_6":
            k, v = process_race_recode_6(value).up()
            d[k] = v
        elif field == "hispanic_origin":
            k, v = process_hispanic_origin(value).up()
            d[k] = v
        elif field == "hispanic_origin_race_recode":
            k, v = process_hispanic_origin_race_recode(value).up()
            d[k] = v
        elif field == "race_recode_40":
            k, v = process_race_recode_40(value).up()
            d[k] = v
        elif field == "decedents_usual_occindu_occupation_4_digit_code":
            k, v = process_decedent_occupation_code(value).up()
            d[k] = v
        elif field == "decedents_usual_occindu_occupation_recode":
            k, v = process_decedent_occupation_recode(value).up()
            d[k] = v
        elif field == "decedents_usual_occindu_industry_4_digit_code":
            k, v = process_decedent_industry_code(value).up()
            d[k] = v
        elif field == "decedents_usual_occindu_industry_recode":
            k, v = process_decedent_industry_recode(value).up()
            d[k] = v

    return Ok(d)


def load_file(path: Path) -> list[str]:
    rlines_task = progress.add_task(
        "> Pulling lines from file, this may take a while", total=None
    )
    with open(path, "r") as f:
        lines = f.readlines()

    progress.remove_task(rlines_task)
    return lines


@propagate_result
def parallel_process_lines(
    path: Path,
) -> Result[pl.DataFrame, str]:
    lines = load_file(path)

    # Evenly distribute the lines to each process
    lines_per_process = len(lines) // MAX_PARALLEL_PROCESSING
    lines_per_process = max(1, lines_per_process)

    # Split the lines into batches
    batches = []
    for i in range(0, len(lines), lines_per_process):
        batch = lines[i : i + lines_per_process]
        batches.append(batch)

    print(f"Processing {len(lines)} lines in {len(batches)} batches")
    print(f"{MAX_PARALLEL_PROCESSING} parallel processes will be used")

    futures: list[
        tuple[Future[Result[pl.DataFrame, str]], TaskID]
    ] = []  # keep track of the jobs

    with multiprocessing.Manager() as manager:
        _progress = manager.dict()
        overall_progress_task = progress.add_task(
            "> Processing lines", total=len(lines)
        )

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=MAX_PARALLEL_PROCESSING
        ) as pool:
            for i, batch in enumerate(batches):
                batch_task = progress.add_task(
                    f"> > Processing batch {i + 1}", total=len(batch)
                )
                future = pool.submit(process_lines_batch, batch, _progress, batch_task)
                futures.append((future, batch_task))

            # monitor the progress:
            while (n_finished := sum([future.done() for future, _ in futures])) < len(
                futures
            ):
                progress.update(
                    overall_progress_task, completed=n_finished, total=len(futures)
                )
                for task_id, update_data in _progress.items():
                    latest = update_data["progress"]
                    total = update_data["total"]
                    # update the progress bar for this task:
                    progress.update(
                        task_id,
                        completed=latest,
                        total=total,
                        visible=latest < total,
                    )

            results = [future.result() for future, _ in futures]

    results = [r.up() for r in results]
    df = pl.concat(results).cast(dataset_schema)  # type: ignore

    return Ok(df)


@propagate_result
def process_lines_batch(
    lines: list[str], progress: DictProxy, task_id: TaskID
) -> Result[pl.DataFrame, str]:
    n = 0

    temp = []
    for line in lines:
        n += 1
        progress[task_id] = {"progress": n, "total": len(lines)}

        processed_line = process_line(line).up()
        #
        # Age
        final_age: int | tuple[int, int] | None = (
            processed_line.get("detail_age")
            or processed_line.get("infant_age_recode_22")
            or processed_line.get("age_recode_52")
            or processed_line.get("age_recode_27")
            or processed_line.get("age_recode_12")
            or None
        )
        age_lower_bound = None
        age_upper_bound = None
        if (
            final_age
            and isinstance(final_age, tuple)
            and all(isinstance(i, int) for i in final_age)
        ):
            age_lower_bound, age_upper_bound = final_age
        elif final_age and isinstance(final_age, int):
            age_lower_bound = final_age
            age_upper_bound = final_age
        #
        # Entity Axis Conditions
        entity_axis_conditions = []
        for i in range(1, 21):
            entity_axis_condition = processed_line.get(f"entity_axis_condition_{i}")
            if entity_axis_condition:
                entity_axis_conditions.append(entity_axis_condition)
        #
        # Record Axis Conditions
        record_axis_conditions = []
        for i in range(1, 21):
            record_axis_condition = processed_line.get(f"record_axis_condition_{i}")
            if record_axis_condition:
                record_axis_conditions.append(record_axis_condition)
        #
        # Remove unneeded fields
        for k in list(processed_line.keys()):
            if k.startswith("entity_axis_condition_") or k.startswith(
                "record_axis_condition_"
            ):
                maybe_del(processed_line, k)

        maybe_del(processed_line, "detail_age")
        maybe_del(processed_line, "infant_age_recode_22")
        maybe_del(processed_line, "age_recode_52")
        maybe_del(processed_line, "age_recode_27")
        maybe_del(processed_line, "age_recode_12")
        maybe_del(processed_line, "education_reporting_flag")
        maybe_del(processed_line, "age_substitution_flag")
        maybe_del(processed_line, "number_of_entity_axis_conditions")
        maybe_del(processed_line, "number_of_record_axis_conditions")
        maybe_del(processed_line, "race_imputation_flag")
        #
        # Combine all the processed fields
        row = {
            **processed_line,
            "age_lower_bound": age_lower_bound,
            "age_upper_bound": age_upper_bound,
            "entity_axis_conditions": entity_axis_conditions,
            "record_axis_conditions": record_axis_conditions,
        }
        temp.append(row)

    df = pl.DataFrame(data=temp, infer_schema_length=None)
    return Ok(df)


if __name__ == "__main__":
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(
            "{task.completed}/{task.total}",
            style="green",
        ),
        TimeRemainingColumn(),
        transient=True,
        refresh_per_second=1,
    ) as p:
        progress = p

        task = progress.add_task("[red]Processing dataset...", limit=None)
        df = parallel_process_lines(DATASET_PATH).unwrap()
        df.write_parquet(OUTPUT_PATH)
