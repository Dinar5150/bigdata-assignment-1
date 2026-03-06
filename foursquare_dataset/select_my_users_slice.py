import pandas as pd
import traceback

MAX_STD = 103
USERS_PERC = 0.25
SID = input(f"Enter your SID [0-{MAX_STD-1}]: ")
try:
    SID = int(SID)
    if SID < 0 or SID >= MAX_STD:
        print(f"SID should be in the range [0-{MAX_STD-1}].")
    else:
        def select_random_data_with_seed(dataframe, num_samples, seed):
            random_selection = dataframe.sample(n=num_samples, random_state=seed)
            return random_selection
        
        print("Reading the file 'users.txt'...")
        df = pd.read_csv("users.txt")
        
        print("Selecting the slice of users...")
        users_num_to_select = int(USERS_PERC * len(df))
        selected_df = select_random_data_with_seed(df, users_num_to_select, seed=SID)
        print(f"Selected DataFrame rows with seed {SID}:\n{selected_df}")
        
        print("Persisting the selected slice of users...")
        file_path = 'my_users.csv'
        selected_df.to_csv(file_path, index=False)
        print(f"The selected slice is successfully saved to {file_path}")
except ValueError:
    print(f"SID = {SID} is not correct, valid SID should be in the range [0-{MAX_STD-1}]!")
except:
    print(f"\033[91m{traceback.format_exc()}\033[0m")
