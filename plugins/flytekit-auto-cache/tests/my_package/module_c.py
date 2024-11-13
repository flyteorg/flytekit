import my_dir

def third_helper():
    print("Third helper")

class DummyClass:
    def dummy_method(self) -> str:
        my_dir.module_in_dir.other_helper_in_directory()
        return "Hello from dummy method!"

    def other_dummy_method(self):
        from module_d import fourth_helper
        print("Other dummy method")
        fourth_helper()
