import json

def main(node_data: list[dict]) -> dict[str, dict[str, str]]:
    flat_data: dict[str, dict[str, str]] = {}
    none_count: int = 0

    for issue in node_data:
        if type(issue) is not dict:
            continue

        issue_id = issue.get("id", "")
        issue_text = issue.get("bodyText", "")
        issue_creation = issue.get("createdAt", "")
        issue_author = issue.get("author", {})
        issue_author_login = issue_author.get("login", "") if issue_author else ""
        issue_author_location = issue_author.get("standardised_location", "") if issue_author else ""

        if issue_author is not None:
            flat_data.setdefault(issue_id, {
                "created_at": issue_creation,
                "author": issue_author_login,
                "author_location": issue_author_location,
                "type": "issue",
                "text": issue_text
            })
        else:
            none_count += 1

        for comment in issue.get("comments", {}).get("nodes", []):
            comment_id = comment.get("id", "")
            comment_text = comment.get("bodyText", "")
            comment_creation = comment.get("createdAt", "")
            comment_author = comment.get("author", {})
            comment_author_login = comment_author.get("login", "") if comment_author else ""
            comment_author_location = comment_author.get("standardised_location", "") if comment_author else ""

            if comment_author is not None:
                flat_data.setdefault(comment_id, {
                    "created_at": comment_creation,
                    "author": comment_author_login,
                    "author_location": comment_author_location,
                    "type": "comment",
                    "parent_issue_id": issue_id,
                    "text": comment_text
                })
            else:
                none_count += 1

    print(f"Total entries with no author: {none_count}")
    return flat_data
if __name__ == "__main__":
    with open("data/processed/issues_data_10k_processed_id.json", "r") as f:
        node_data = json.load(f)

    flat_data = main(node_data)

    with open("data/processed/flat_nlp_data.jsonl", "w", encoding="utf-8") as f:
        for key, row in flat_data.items():
            entry = {key: row}
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
