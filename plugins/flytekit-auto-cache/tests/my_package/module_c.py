import my_dir

def third_helper():
    print("Third helper")

class DummyClass:
    some_attr = "some_custom_attr"

    def dummy_method(self) -> str:
        my_dir.other_helper_in_directory()
        import numpy as np
        print(np.mean(np.array([1, 2, 3, 4, 5])))
        return f"{self.some_attr}"

    def other_dummy_method(self):
        from module_d import fourth_helper
        from PIL import Image
        img = Image.new("RGB", (100, 100), color="white")
        print(img.info)
        fourth_helper()
