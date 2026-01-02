import requests
import time
import json
import time
import dotenv
import os

def check_rate_limit(session: requests.Session, github_url: str) -> None:
    query = """
    {
        rateLimit {
            remaining
            resetAt
        }
    }
    """
    response: requests.Response = session.post(
        github_url,
        json={"query": query}
    )
    j = response.json()
    print(f'Remaining rate limit: {j["data"]["rateLimit"]["remaining"]}')

    if response.status_code == 200:
        if "errors" in j:
            raise Exception(f"Query failed to run with errors: {j['errors']}")

        rate_limit = j["data"]["rateLimit"]
        remaining = int(rate_limit["remaining"])
        reset_at = rate_limit["resetAt"]

        if remaining < 100:
            reset_time = time.strptime(reset_at, "%Y-%m-%dT%H:%M:%SZ")
            reset_timestamp = time.mktime(reset_time)
            current_timestamp = time.time()
            sleep_time = reset_timestamp - current_timestamp + 5  
            print(f"Rate limit nearly exceeded. Sleeping for {sleep_time} seconds, until {reset_at}.")
            time.sleep(sleep_time)
    else:
        raise Exception(
            f"Query failed to run by returning code of {response.status_code}. {query}"
        )

def main(session: requests.Session, github_url: str, max_nodes: int) -> None:
    all_nodes = fetch_issues_paginated(session, github_url, max_nodes=max_nodes)

    with open("data/raw/issues_data_10k.json", "a") as f:
        json.dump(all_nodes, f, indent=2)

def fetch_issues_paginated(session: requests.Session, github_url: str, max_nodes: int=10000) -> list:
    nodes = []
    rate_counter = 0
    cursor = None
    
    while len(nodes) < max_nodes:
        query = """
        {
          repository(owner: "huggingface", name: "transformers") {
            issues(first: 100, after: %s) {
              nodes {
                title
                bodyText
                createdAt

                labels(first: 5) {
                  nodes {
                    name
                  }
                }

                author { ... on User { login location } }

                comments(first: 25) {
                  nodes {
                    bodyText
                    createdAt
                    author { ... on User { login location } }
                  }
                }
              }
              pageInfo {
                endCursor
                hasNextPage
              }
            }
          }
        }
        """ % (f'"{cursor}"' if cursor else "null")
        
        try:
            response = session.post(github_url, json={"query": query})
        except (requests.exceptions.RequestException) as e:
            print(f"Request error: {e}. Retrying in 10 seconds...")
            time.sleep(10)
            continue
        print(f"Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}. Retrying in 10 seconds...")
            time.sleep(10)
            continue
        
        try:
            json_response = response.json()
        except json.JSONDecodeError:
            print("Error: Invalid JSON response. Retrying in 10 seconds...")
            time.sleep(10)
            continue
        
        if "errors" in json_response:
            print(f"GraphQL errors: {json_response['errors']}")
            break
        
        data = json_response["data"]["repository"]["issues"]
        
        nodes.extend(data["nodes"])
        print(f"Fetched {len(data['nodes'])} nodes. Total: {len(nodes)}")
        
        if not data["pageInfo"]["hasNextPage"]:
            break
        
        cursor = data["pageInfo"]["endCursor"]

        rate_counter += 1
        if rate_counter % 10 == 0:
            check_rate_limit(session, github_url)

        time.sleep(1)
    
    return nodes[:max_nodes]


if __name__ == "__main__":
    dotenv.load_dotenv()
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    if not GITHUB_TOKEN:
        raise RuntimeError("GITHUB_TOKEN is not set; create a .env with GITHUB_TOKEN=<token>")
    github_url = "https://api.github.com/graphql"

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Content-Type": "application/json"
        }
    )

    main(session, github_url, max_nodes=10000)

 