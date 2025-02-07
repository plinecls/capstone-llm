import argparse
import requests
from typing import List

import logging
logger = logging.getLogger(__name__)

def get_questions_by_tag(tag, pagesize=100):
    """Retrieve questions from Stack Overflow by tag."""
    url = "https://api.stackexchange.com/2.3/questions"
    params = {
        "order": "desc",
        "sort": "creation",
        "tagged": tag,
        "site": "stackoverflow",
        "pagesize": pagesize
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    question_ids = [q["question_id"] for q in data.get("items", [])]

    return data, question_ids

def get_answers_for_questions(question_ids):
    """Retrieve answers for a list of question IDs."""
    url = f"https://api.stackexchange.com/2.3/questions/{';'.join(map(str, question_ids))}/answers"
    params = {
        "order": "desc",
        "sort": "votes",
        "site": "stackoverflow",
        "filter": "withbody"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    return data

def write_json(environment: str, tag: str, user: str):

    



if __name__ == "__main__":
    tag = "pyspark"
    questions, question_ids = get_questions_by_tag(tag)
    print(question_ids)
    
    answers = get_answers_for_questions(question_ids)
    print(answers)
    
    # print("\nRelevant Answers:")
    # for q_id, answer in answers.items():
    #     print(f"Question {q_id}: {answer[:200]}...")  # Print a snippet of the answer

# def ingest(tag: str):
#     pass

# def main():
#     parser = argparse.ArgumentParser(description="stackoverflow ingest")
#     parser.add_argument(
#         "-t", "--tag", dest="tag", help="Tag of the question in stackoverflow to process",
#         default="python-polars", required=False
#     )
#     args = parser.parse_args()
#     logger.info("Starting the ingest job")

#     ingest(args.tag)


# if __name__ == "__main__":
#     main()
