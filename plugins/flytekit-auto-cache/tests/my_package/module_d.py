def fourth_helper():
    print("Fourth helper")
    import yaml
    print(yaml.__version__)
    import my_dir.module_in_dir as mod
    print(mod.SOME_OTHER_CONSTANT)
