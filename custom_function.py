import os 
import requests
import pandas as pd
import numpy as np
from itertools import product
import warnings
import time
    

# define requets workflow
def requests_get(url,headers):
    """
    Return requests.model.Response if and only if the status code is 200.
    """
    print(f"requesting {url}")
    try:
        response = requests.get(url,headers = headers)
        if response.status_code != 200:
            print(f"A {response.status_code} error occured when requesting the url")
            return None
        else:
            # when successively returned
            return response
    except:
        print("System error with the `requests` module")
        return None



# set the dataset id from Nomix
dataset_dict = {'Demography and migration': None,
 'TS001': 'Number of usual residents in households and communal establishments',
 'TS002': 'Legal partnership status',
 'TS003': 'Household composition',
 'TS004': 'Country of birth',
 'TS005': 'Passports held',
 'TS006': 'Population density',
 'TS007': 'Age by single year of age',
 'TS007A': 'Age by five-year age bands',
 'TS008': 'Sex',
 'TS009': 'Sex by single year of age',
 'TS010': 'Living arrangements',
 'TS011': 'Households by deprivation dimensions',
 'TS012': 'Country of birth (detailed)',
 'TS013': 'Passports held (detailed)',
 'TS015': 'Year of arrival in UK',
 'TS016': 'Length of residence',
 'TS017': 'Household size',
 'TS018': 'Age of arrival in the UK',
 'TS019': 'Migrant Indicator',
 'TS020': 'Number of non-UK short-term residents by sex',
 'TS041': 'Number of Households',
 'UK armed forces veterans': None,
 'TS071': 'Previously served in the UK armed forces',
 'TS072': 'Number of people in household who have previously served in UK armed forces',
 'TS073': 'Population who have previously served in UK armed forces in communal establishments and in households',
 'TS074': 'Household Reference Person indicator of previous service in UK armed forces',
 'Ethnicity, Identity, Language and Religion': None,
 'TS021': 'Ethnic group',
 'TS022': 'Ethnic group (detailed)',
 'TS023': 'Multiple ethnic group',
 'TS024': 'Main language (detailed)',
 'TS025': 'Household language',
 'TS026': 'Multiple main languages in households',
 'TS027': 'National identity - UK',
 'TS028': 'National identity (detailed)',
 'TS029': 'Proficiency in english',
 'TS030': 'Religion',
 'TS031': 'Religion (detailed)',
 'TS075': 'Multi religion households',
 'Welsh language in Wales': None,
 'TS032': 'Welsh language skills (detailed)',
 'TS033': 'Welsh language skills (speaking)',
 'TS034': 'Welsh language skills (writing)',
 'TS035': 'Welsh language skills (reading)',
 'TS036': 'Welsh language skills (understanding)',
 'TS076': 'Welsh language skills (speaking) by single year of age',
 'Work and Travel': None,
 'TS058': 'Distance travelled to work',
 'TS059': 'Hours worked',
 'TS060': 'Industry',
 'TS061': 'Method of travel to work',
 'TS062': 'NS-SeC',
 'TS063': 'Occupation',
 'TS064': 'Occupation - minor groups',
 'TS065': 'Unemployment history',
 'TS066': 'Economic activity status',
 'Housing': None,
 'TS044': 'Accommodation type',
 'TS045': 'Car or van availability',
 'TS046': 'Central heating',
 'TS047': 'Communal establishment residents by age and sex',
 'TS048': 'Communal establishment management and type',
 'TS050': 'Number of bedrooms',
 'TS051': 'Number of rooms',
 'TS052': 'Occupancy rating for bedrooms',
 'TS053': 'Occupancy rating for rooms',
 'TS054': 'Tenure',
 'TS055': 'Purpose of second address',
 'TS056': 'Second address indicator',
 'Sexual Orientation and Gender Identity': None,
 'TS079': 'Sexual orientation (detailed)',
 'TS070': 'Gender identity (detailed)',
 'TS077': 'Sexual orientation',
 'TS078': 'Gender identity',
 'Education': None,
 'TS067': 'Highest level of qualification',
 'TS068': 'Schoolchildren and full-time students',
 'Health': None,
 'TS037': 'General health',
 'TS038': 'Disability',
 'TS039': 'Provision of unpaid care',
 'TS040': 'Number of disabled people in the household',
 'TS037ASP': 'General health - age-standardised proportions',
 'TS038ASP': 'Disability - age-standardised proportions',
 'TS039ASP': 'Provision of unpaid care - age-standardised proportions'}
# nomis dataset_dict
dataset_id = pd.DataFrame.from_dict(dataset_dict,orient='index')
dataset_id.columns = ["variable"]
# drop the header with "None"
dataset_id = dataset_id[~dataset_id['variable'].isna()]
# string formatting
dataset_id['variable'] = dataset_id['variable'].str.lower()

# define lookup dataset id 
def lookup_dataset(keyword = None):
    """
    Return a list of variables if no keyword is passed. 
    Otherwise, return the pd.DF dataset id that matches with the variable
    """
    if keyword is not None:
        keyword = keyword.lower().strip()
        filter_dataset = dataset_id.str.contains(keyword)
        if len(filter_dataset) != 0:
            return filter_dataset
    
    print(f"The list of variable available: {dataset_id['variable'].values}")
    return None

# request census data
def requests_census2021_api(area_code: list,datasetId = "TS009", version = 1, area_type = "msoa", edition = 2021):
    """
    Send API request for retrieving ONS Census 2021.
    Depending on the dataset, the version and edition available is different.
    Simiarly, the data may also be available only for certain area type.
    """
    # get the user agent 
    try: 
        header = globals().get("header")
    except:
        print("Please define a global variable header in the form of:\n{'user-agent':'botName (Org Email)'}")
        return None
    
    # parse area list into a string
    area_str = ",".join(area_code)
    
    # request api
    url = f"https://api.beta.ons.gov.uk/v1/datasets/{datasetId}/editions/{edition}/versions/{version}/json?area-type={area_type},{area_str}"
    res = requests_get(url, header)
    time.sleep(0.2)

    # define parse result
    def parse_api_result(res):
        # get the area code (index)
        index = res.json()["dimensions"][0]['options']
        # get the combination(s) of disaggregate (columns)
        disaggregate_list = []
        # loop through all the dimension level i.e. gender, age etc.
        for dimension_cat in res.json()["dimensions"][1:]:
            groups = [dim['label'] for dim in dimension_cat['options']]
            disaggregate_list.append(groups)
        cols = ['.'.join(combination) for combination in product(*disaggregate_list)]

        # disaggregate = res.json()["dimensions"][1]['options']
        data_point = res.json()["observations"]

        ## parse into a pandas dataframe
        # reshape observation into list of rows
        dim = (len(index),len(cols) )
        array = np.reshape(data_point, newshape = dim)
        # transpose the array
        array_t = array.transpose()

        df = pd.json_normalize(index)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for i in range(len(cols)):
                # label = disaggregate[i]['label']
                label = cols[i]
                df[label] = array_t[i]
        return df

    # parse the json into dataframe
    if res is not None:
        df = parse_api_result(res)
        return df
    
    else:
        # check if the version/edition/dataset is valid
        q = f"https://api.beta.ons.gov.uk/v1/datasets/{datasetId}/editions/{edition}/versions/{version}"
        res = requests_get(q, header)
        if res.status_code == 404:
            error_msg = res.text.replace("\n","")
            print(error_msg)

        elif res.status_code == 200:
            # loop over all the area code parsed in
            # instanstiate area 
            df = pd.DataFrame()

            # check if the area-type and area code is available
            for area in area_code:
                q = f"https://api.beta.ons.gov.uk/v1/datasets/{datasetId}/editions/{edition}/versions/{version}/json?area-type={area_type},{area}"
                res = requests_get(q, header) # note that it will only return a request is status code = 200
                try:
                    new_data = parse_api_result(res)
                    df = pd.concat([df,new_data], ignore_index=True)
                
                except AttributeError:
                    print(f"The API is not able to locate area {area_type} {area}")

            if len(df) > 0:
                area_succ = df['id']
                area_fail = set(area_code).difference(set(area_succ))
                print(f"Successfully retrieved datapoint for area {list(df['id'])}\n{list(area_fail)} cannot be loacted")

        
        else:
            print(f"{res.status_code} error occured: {res.text}")

        return None
