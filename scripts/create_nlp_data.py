import json

def main(node_data: dict[str, dict], text_data: list[dict]) -> dict[str, dict]:
    cleaned_ids = {item.get("id") for item in text_data if item.get("id")}
    node_data = {key: value for key, value in node_data.items() if key in cleaned_ids}

    for item in text_data:
        item_id = item.get("id")
        cleaned_text = item.get("text")
        
        if item_id and item_id in node_data:
            node_data[item_id]["text"] = cleaned_text

    return node_data

if __name__ == "__main__":
    node_data = {}
    with open("data/processed/flat_nlp_data.jsonl", "r") as f:
        for line in f:
            node_data.update(json.loads(line))

    text_data = []
    with open("data/processed/texts_only_with_ids_cleaned.jsonl", "r") as f:
        for line in f:
            text_data.append(json.loads(line))

    data = main(node_data, text_data)

    with open("data/processed/final_nlp_data.jsonl", "w", encoding="utf-8") as f:
        for key, row in data.items():
            entry = {key: row}
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
