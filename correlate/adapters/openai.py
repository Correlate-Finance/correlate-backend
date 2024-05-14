from django.conf import settings
import openai
import json


class OpenAIAdapter:
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        if self.api_key is None:
            raise ValueError("OPENAI_API_KEY is not set in settings.py")
        self.client = openai.OpenAI(api_key=self.api_key)

    # TODO: Add internal name to the input text so that we can map to the correct dataset
    # TODO: Add better error states so that consumer can know what went wrong
    def generate_report(self, input_text: str):
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "Your job is to help hedge fund analysts find out if any data will be useful to predict a company's performance. I will feed you a company and a brief description of the company. Then I will provide a list of datasets which include their name, series id and the correlation between the revenue of the company and the dataset. You will be tasked with picking out any datasets that you think are relevant for the company based on the company description, the dataset name and the correlation.  You will also need to provide a rationale for each dataset. You should return your response in a JSON format. \n A sample schema is: \n { \n 'relevant_datasets': [ \n { \n 'name': 'Dataset Name', \n 'series_id': 'Series ID', \n 'correlation': 0.5, \n 'rationale': 'Rationale for why this dataset is relevant' \n } \n ] \n } \n",
                },
                {
                    "role": "user",
                    "content": input_text,
                },
            ],
            # Defaults picked up from the playground
            temperature=1,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
            response_format={"type": "json_object"},
        )

        # Parse the json response
        choices = response.choices
        if len(choices) == 0:
            return None

        content = choices[0].message.content
        if content is None:
            return None

        data = json.loads(content)
        if "relevant_datasets" not in data:
            return None

        return data["relevant_datasets"]
