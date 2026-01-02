import json
import pandas as pd
import time
import csv
import os


def remove_authorless_comments(issue: dict) -> None:
    if "comments" in issue:
        filtered_comments = [
            comment for comment in issue["comments"]["nodes"]
            if "author" in comment and comment["author"] != {}
        ]
        issue["comments"]["nodes"] = filtered_comments

def standardise_author_locations(issue: dict, location_lookup: pd.DataFrame) -> None:
    author = issue.get("author")
    if author and "location" in author:
        raw_location = author.get("location", None)
        if raw_location is None or raw_location == "":
            author["standardised_location"] = None
        else:
            standardised_location = location_lookup.get(raw_location)
            if standardised_location:
                author["standardised_location"] = standardised_location
    
    if "comments" in issue:
        if len(issue["comments"]["nodes"]) == 0:
            return
        
        for comment in issue["comments"]["nodes"]:
            comment_author = comment.get("author")

            if comment_author == {} or comment_author is None:    # Bot or deleted user instances
                continue
            if comment_author.get("location") is None:
                comment_author["standardised_location"] = None
            if comment_author and "location" in comment_author and comment_author["location"]:
                raw_location = comment_author["location"]
                standardised_location = location_lookup.get(raw_location)
                
                if standardised_location:
                    comment_author["standardised_location"] = standardised_location
                else:
                    comment_author["standardised_location"] = None
    
    pass

def main(node_data: list, location_lookup: dict) -> None:
    for issue in node_data:
        remove_authorless_comments(issue)
        standardise_author_locations(issue, location_lookup)

    with open("data/processed/issues_data_10k_processed.json", "a") as f:
        json.dump(node_data, f, indent=2)


if __name__ == "__main__":
    with open("data/raw/issues_data_10k.json", "r") as f:
        node_data = json.load(f)

    with open("data/processed/author_locations_processed.csv", "r") as f:
        location_lookup = pd.read_csv(f)
        location_lookup = location_lookup.set_index("location_raw")["country_code"].to_dict()

    main(node_data, location_lookup)    