from sklearn.preprocessing import StandardScaler

def helper_in_directory():
    print("Helper in directory")

def other_helper_in_directory():
    data = [[1, 2], [3, 4], [5, 6]]
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(data)
    print(scaled_data)
