import pandas as pd
import traceback

USERS_PERC = 0.01
SID = input("Enter your SID: ")
try:
    SID = int(SID)

    print("Reading the file 'users.txt'...")
    df = pd.read_csv("users.txt")

    print("Selecting a small sample of users...")
    users_num_to_select = int(USERS_PERC * len(df))
    selected_df = df.sample(n=users_num_to_select, random_state=SID)
    print(f"Selected {len(selected_df)} users (5% sample) with seed {SID}:\n{selected_df}")

    file_path = 'my_users.csv'
    selected_df.to_csv(file_path, index=False)
    print(f"Saved to {file_path}")
except:
    print(f"\033[91m{traceback.format_exc()}\033[0m")
