"""
prepare_data.py

Reads the full dataset files in chunks and filters only rows
belonging to users from my_users.csv. Saves the filtered data
as my_*.tsv files. Memory-safe for large files (~20M rows).
"""
import pandas as pd

CHUNK = 500_000

print("Loading my user IDs...")
my_users = set(pd.read_csv("foursquare_dataset/my_users.csv")["userid"].values)
print(f"Total users in slice: {len(my_users)}")

# --- checkins ---
print("\nFiltering checkins...")
out = open("foursquare_dataset/my_checkins_anonymized.tsv", "w", encoding="utf-8", errors="replace")
count = 0
for chunk in pd.read_csv(
    "foursquare_dataset/checkins_anonymized.txt",
    sep="\t", header=None,
    names=["user_id", "venue_id", "utc_time", "timezone_offset"],
    chunksize=CHUNK
):
    filtered = chunk[chunk["user_id"].isin(my_users)]
    filtered = filtered[pd.to_datetime(filtered["utc_time"], errors="coerce", format="%a %b %d %H:%M:%S %z %Y").notna()]
    filtered.to_csv(out, sep="\t", header=False, index=False, lineterminator="\n")
    count += len(filtered)
    print(f"\r  checkins: {count} rows kept", end="", flush=True)
out.close()
print(f"\nDone: {count} checkins saved.")

# --- friendships_before ---
print("\nFiltering friendships_before...")
out = open("foursquare_dataset/my_friendships_before.tsv", "w", encoding="utf-8", errors="replace")
count = 0
for chunk in pd.read_csv(
    "foursquare_dataset/friendship_before_old.txt",
    sep="\t", header=None,
    names=["user_id", "friend_id"],
    chunksize=CHUNK
):
    filtered = chunk[(chunk["user_id"].isin(my_users)) & (chunk["friend_id"].isin(my_users))]
    filtered.to_csv(out, sep="\t", header=False, index=False, lineterminator="\n")
    count += len(filtered)
    print(f"\r  friendships_before: {count} rows kept", end="", flush=True)
out.close()
print(f"\nDone: {count} friendships_before saved.")

# --- friendships_after ---
print("\nFiltering friendships_after...")
out = open("foursquare_dataset/my_friendships_after.tsv", "w", encoding="utf-8", errors="replace")
count = 0
for chunk in pd.read_csv(
    "foursquare_dataset/friendship_after_new.txt",
    sep="\t", header=None,
    names=["user_id", "friend_id"],
    chunksize=CHUNK
):
    filtered = chunk[(chunk["user_id"].isin(my_users)) & (chunk["friend_id"].isin(my_users))]
    filtered.to_csv(out, sep="\t", header=False, index=False, lineterminator="\n")
    count += len(filtered)
    print(f"\r  friendships_after: {count} rows kept", end="", flush=True)
out.close()
print(f"\nDone: {count} friendships_after saved.")

# --- POIs ---
print("\nFiltering POIs...")
checkins = pd.read_csv(
    "foursquare_dataset/my_checkins_anonymized.tsv",
    sep="\t", header=None,
    names=["user_id", "venue_id", "utc_time", "timezone_offset"]
)
my_venues = set(checkins["venue_id"].values)
print(f"Unique venues in my checkins: {len(my_venues)}")

out = open("foursquare_dataset/my_POIs.tsv", "w", encoding="utf-8", errors="replace")
count = 0
for chunk in pd.read_csv(
    "foursquare_dataset/POIs.txt",
    sep="\t", header=None,
    names=["venue_id", "latitude", "longitude", "category", "country"],
    chunksize=CHUNK,
    encoding="utf-8", encoding_errors="replace"
):
    filtered = chunk[chunk["venue_id"].isin(my_venues)]
    filtered.to_csv(out, sep="\t", header=False, index=False, lineterminator="\n")
    count += len(filtered)
    print(f"\r  POIs: {count} rows kept", end="", flush=True)
out.close()
print(f"\nDone: {count} POIs saved.")

print("\nAll data preparation finished!")
