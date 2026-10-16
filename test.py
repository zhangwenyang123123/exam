import requests
res = requests.post("http://127.0.0.1:8000/query", json={"question":"锂的价格是多少？"})
print(res.json())
