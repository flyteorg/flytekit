import base64
import json

# runtime = '{"pip": ["git+https://github.com/flyteorg/flytekit.git@ray#egg=flytekitplugins-ray&subdirectory=plugins/flytekit-ray","git+https://github.com/flyteorg/flytekit@ray", "git+https://github.com/flyteorg/flyteidl@ray"]}'

a = json.dumps({"pip": ["numpy", "pandas"]})
b = base64.b64encode(a.encode())

encoding = "utf-8"
print(b.decode(encoding))

print(b)
print(type(b))
print(str(b) == "eyJwaXAiOiBbIm51bXB5IiwgInBhbmRhcyJdfQ==")
print(base64.decodebytes(b))
