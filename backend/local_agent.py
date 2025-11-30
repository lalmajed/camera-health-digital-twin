import requests
import json

print("Testing debug_site with different methods...\n")

# Method 1: json parameter
print("Method 1: Using json parameter")
response1 = requests.post(
    'http://localhost:8080/project::debug_site',
    json=["RUHSM336"]
 )
print(f"Status: {response1.status_code}")
print(f"Response: {response1.text[:200]}\n")

# Method 2: data parameter with manual JSON
print("Method 2: Using data parameter")
response2 = requests.post(
    'http://localhost:8080/project::debug_site',
    data='["RUHSM336"]',
    headers={'Content-Type': 'application/json'}
 )
print(f"Status: {response2.status_code}")
print(f"Response: {response2.text[:200]}\n")

# Method 3: data parameter with json.dumps
print("Method 3: Using json.dumps")
response3 = requests.post(
    'http://localhost:8080/project::debug_site',
    data=json.dumps(["RUHSM336"] ),
    headers={'Content-Type': 'application/json'}
)
print(f"Status: {response3.status_code}")
print(f"Response: {response3.text[:200]}")
