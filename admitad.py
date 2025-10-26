

import base64
import requests

# Replace with your actual Admitad client ID and client secret
client_id = "ROXoRRB1L1DRdgJdzXDuNhEzg7x86R"
client_secret = "9BbR4PGZ8kO6rcznoLfa5wgpVGZ8gp"

# Combine client ID and secret with a colon
auth_string = f"{client_id}:{client_secret}"

# Base64 encode the string
encoded_auth_string = "Uk9Yb1JSQjFMMURSZGdKZHpYRHVOaEV6Zzd4ODZSOjlCYlI0UEdaOGtPNnJjem5vTGZhNXdncFZHWjhncA==" # base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

# Construct the Authorization header
headers = {
    'Authorization': f'Basic {encoded_auth_string}',
    'Content-Type': 'application/x-www-form-urlencoded' # Required for token requests
}

# Example: Requesting an access token
token_url = 'https://api.admitad.com/token/'
data = {
    'grant_type': 'client_credentials' # Or other grant types depending on your needs
}

try:
    response = requests.post(token_url, headers=headers, data=data)
    response.raise_for_status() # Raise an exception for HTTP errors
    token_info = response.json()
    print("Access Token Info:")
    print(token_info)

    # You can then use the 'access_token' from token_info for subsequent API calls
    access_token = token_info.get('access_token')
    if access_token:
        print(f"\nObtained Access Token: {access_token}")
        # Example of using the access token for another API call
        # api_url = 'https://api.admitad.com/me/'
        # auth_headers = {'Authorization': f'Bearer {access_token}'}
        # user_info_response = requests.get(api_url, headers=auth_headers)
        # print("\nUser Info:")
        # print(user_info_response.json())

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print(f"Response content: {e.response.text}")
