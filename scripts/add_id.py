import json

def add_id(issue: str, issue_id: str) -> str:
    comment_id = 0
    issue["id"] = f"{issue_id}"

    if "comments" not in issue:
        return
        
    if len(issue["comments"]["nodes"]) == 0:
        return

    for comment in issue["comments"]["nodes"]:
        comment["id"] = f"{issue_id}_{comment_id}"
        comment_id += 1

def main(node_data: dict) -> None:
    issue_id = 0
    for issue in node_data:
        add_id(issue, issue_id)
        issue_id += 1

    with open("data/processed/issues_data_10k_processed_id.json", "a") as f:
        json.dump(node_data, f, indent=2)

if __name__ == "__main__":
    with open("data/processed/issues_data_10k_processed.json", "r") as f:
        node_data = json.load(f)

    main(node_data)