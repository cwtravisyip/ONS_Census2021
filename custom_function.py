import os 
import requests
import pandas as pd
import numpy as np
from itertools import product
from scrapy import Selector
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


# define lookup dataset id 
def lookup_dataset(keyword = None):
    """
    Return a list of variables if no keyword is passed. 
    Otherwise, return the pd.DF dataset id that matches with the variable.
    Note that the result from the function may not be comprehensive.
    """
    if keyword is not None:
        keyword = keyword.lower().strip()
        filter_dataset = dataset_id.str.contains(keyword)
        if len(filter_dataset) != 0:
            return filter_dataset
    
    print(f"The list of variable available: {dataset_id['variable'].values}")
    return None

# request census data
def requests_census2021_api(area_code: list,datasetId = "TS009", version = 1, area_type = "msoa", edition = 2021, verbose = 0):
    """
    Send API request for retrieving ONS Census 2021.
    Depending on the dataset, the version and edition available is different.
    Simiarly, the data may also be available only for certain area type.
    
    Parameters:
    --------
    area_code: list
        A list of area codes for which census 2021 data are to be returned. The area code must match with the area_type
    datasetId: str
        The ID of the variables (i.e. dataset) to request data for. the lookup_dataset function could 
    verbose: int {0,1,2}
        By default, set at 0 where all status are printed.
        =1, only raise print when request error occued
        =2, no meessage is printed
    
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


def get_dataset_info():
    """
    Return a set of dataset, including the variable name, dataset id, latest version and edition.
    """
    # initialise the scrape
    page = 1
    dataset_uri = dict()
    print("Retriveing ONS Census 2021 dataset info. This takes about a minute to complete.")

    while True:
        census_url = f"https://www.ons.gov.uk/search?page={page}&topics=9731,6646,3845,7267,9497,4262,8463,4128,7755,4994,6885,9724,7367,9731,6646,3845,7267,9497,4262,8463,4128,7755,4994,6885,9724,7367"
        response = requests.get(census_url)
        # break the while loop
        if response.status_code != 200:
            break
        sel = Selector(response)
        datasets = sel.xpath("//section[@role='contentinfo']/div[@class='search__results']//li")
        for dataset in datasets:
            uri = dataset.xpath("h3/a/@href").extract()[0]
            label = dataset.xpath("h3/a/text()").get().replace("\n","")
            dataset_uri[uri] = label
        time.sleep(1)
        page +=1

    # parse into dataframe
    df = pd.DataFrame.from_dict(dataset_uri, orient = "index").reset_index()
    df.columns = ['uri','variable']

    # subset for datasets
    df = df[df['uri'].str.contains("^/datasets/")].reset_index(drop = True)

    # parse info from the dataset uri
    df['uri_comp']= df['uri'].str.split("/")
    try:
        uri_comp = list(df['uri_comp'].apply(lambda uri: (uri[2],uri[4],uri[6])))
        dataset_details = pd.DataFrame(uri_comp, columns = ["dataset_id","latest_vers","latest_ed"])

        # return the dataset information
        df = pd.merge(df['variable'], dataset_details, left_index = True, right_index = True)
        return df
    except:
        print("unable to parse the uri")
        return df