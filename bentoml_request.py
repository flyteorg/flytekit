import requests

result = requests.post(
   "https://54lh7vip75.execute-api.us-east-2.amazonaws.com/classify",
   headers={"content-type": "application/json"},
   data="[[5.9, 3, 5.1, 1.8]]",
).text

print(result)
