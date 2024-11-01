import requests, json, re, time, math, heapq, os

def get_token(url):
    
    response = requests.get(url)
    
    if response.status_code == 200:
            data = response.json()
            authorizationToken = data['data']['authorizationToken']
            
            return authorizationToken
    else:
            print("Error:", response.status_code)
            
def download_dataset(url, token):
    
    headers = {
        "authorizationToken": token,
        "Content-Type": "application/json"
    }
    
    payload = {}
    data_list = []
    next_id = ""

    while True:
        payload["next_id"] = next_id

        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            
            dataset_url = data['data']['dataset_url']
            next_id = data['data']['next_id']
            
            response = requests.get(dataset_url, stream=True)
            data_list.extend(json.load(response.raw))
            
            print('next_id: {} message: {}'.format(next_id, data['message']))
            
            if not next_id:
                print("No more records available.")
                break
            
            time.sleep(10)

        elif response.status_code == 401:
            print("Unauthorized. Please check your authorization token.")
            break
        elif response.status_code == 404:
            print("Dataset not found.")
            break
        elif response.status_code == 429:
            print("Rate limit exceeded. Please try again later.")
            break
        else:
            print("Error:", response.status_code)
            break
    
    return data_list

def validate_dataset(data_list):
    
    cleaned_data = []
    for record in data_list:
        if (isinstance(record.get('id'), int) and
            isinstance(record.get('restaurant_name'), str) and
            re.match(r'^[A-Za-z\s]+$', record['restaurant_name']) and
            isinstance(record.get('rating'), float) and 1.00 <= record['rating'] <= 10.00 and
            isinstance(record.get('distance_from_me'), float) and 10.00 <= record['distance_from_me'] <= 1000.00):
            cleaned_data.append(record)
            
    with open("validated_dataset.json", "w") as f:
        json.dump(cleaned_data, f, indent=4)

def find_top_10(file_name):
        
    with open(file_name, "r") as f:
        cleaned_data_list = json.load(f)
    
    for record in cleaned_data_list:
        id = record['id']
        rating = record['rating']
        distance = record['distance_from_me']
        score = (rating * 10 - distance * 0.5 + math.sin(id) * 2) * 100 + 0.5
        record['score'] = round(score/100, 2)
    
    top_10 = heapq.nlargest(10, cleaned_data_list, key=lambda x: (x['score'], x['rating'], x['distance_from_me'], x['restaurant_name']))
    # top_10 = sorted(cleaned_data_list, key=lambda x: (-x['score'], -x['rating'], -x['distance_from_me'], x['restaurant_name']))[:10]
    
    with open("topk_results.json", "w") as f:
        json.dump(top_10, f, indent=4)

        
def test_validate_dataset(url, token):
    
    headers = {
        "authorizationToken": token
    }
    
    with open("validated_dataset.json", "r") as f:
        validation_payload = json.load(f)
        
        validation_response = requests.post(url, headers=headers, json={"data": validation_payload})
        
        if validation_response.status_code == 200:
            data = validation_response.json()
            print('message: {}'.format(data['message']))
            
def test_validate_topk(url, token):
    
    headers = {
        "authorizationToken": token
    }
    
    with open("topk_results.json", "r") as f:
        topk_results_payload = json.load(f)
        
        validation_response = requests.post(url, headers=headers, json={"data": topk_results_payload})
        if validation_response.status_code == 200:
            data = validation_response.json()
            print('message: {}'.format(data['message']))  
def main():
    # Get the API_URL from environment variable
    API_URL = os.getenv("API_URL")
    # API_URL = "https://u8whitimu7.execute-api.ap-southeast-1.amazonaws.com/prod"
    
    authorization_token = get_token(API_URL + "/register")
    
    dataset_url = API_URL + "/download-dataset"
    data_list = download_dataset(dataset_url, authorization_token)
    
    validate_dataset(data_list)
    
    # find_top_10('validated_dataset.json')
    
    # test_validate_dataset(API_URL + "/test/check-data-validation", authorization_token)
    # test_validate_topk(API_URL + "/test/check-topk-sort", authorization_token)

if __name__ == "__main__":
    main()