import time
from flytekit.configuration import Config
from flytekit.remote import FlyteRemote

def test_flyte_client():
    
    try:

        config = Config.for_sandbox()
        remote = FlyteRemote(config=config)
        remote.client
        print("成功連接到 Flyte 服務器！")
    except Exception as e:
        print(f"錯誤發生: {str(e)}")

if __name__ == "__main__":
    test_flyte_client()