import json

def get_text_from_nodes(issue: dict) -> str:
    titles = []
    texts = []
    ids = []

    titles.append(issue.get("title", ""))
    texts.append(issue.get("bodyText", ""))
    ids.append(issue.get("id", ""))

    if "comments" not in issue:
        return (titles, texts, ids)
    if len(issue["comments"]["nodes"]) == 0:
        return (titles, texts, ids)
    for comment in issue["comments"]["nodes"]:
        texts.append(comment.get("bodyText", ""))
        ids.append(comment.get("id", ""))
    
    return (titles, texts, ids)

def main(node_data: list) -> None:
    titles = []
    texts = []
    id_list = []
    for issue in node_data:
        title, text, id_list = get_text_from_nodes(issue)
        text_list = zip(text, id_list)

        for title_string in title:
            if title_string.strip() != "":
                titles.append(title_string)        
        
        for comment, id in text_list:
            if comment.strip() != "": 
                _: dict = {"id": id, "text": comment}
                texts.append(_)
    
    with open("data/processed/titles_only.jsonl", 'w') as f:
        for title in titles:
            f.write(json.dumps(title) + "\n")
            
    with open("data/processed/texts_only_with_ids.jsonl", 'w') as f:
        for text in texts:
            f.write(json.dumps(text) + "\n")

if __name__ == "__main__":
    with open("data/processed/issues_data_10k_processed_id.json", "r") as f:
        node_data = json.load(f)

    main(node_data)