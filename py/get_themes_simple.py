import pandas as pd
import json
import openai
import time
from threading import Lock

def get_theme_simple(hashtags, retries=2, model=None, client=None):
    prompt = """
    You are an expert linguist categorizing TikTok hashtags related to substances and drug use. Categorize each hashtag into EXACTLY ONE of these categories:
    - emotions_and_feelings
    - health_conditions
    - alcohol
    - cannabis
    - cognitive_enhancement
    - commonly_misused_substances
    - consumption_method
    - awareness_and_advocacy
    - other_substances
    - platform
    - substance_effects
    - tobacco_nicotine
    - humor
    - location
    - occupation
    - identity_and_community
    - misc

    Rules:
    - Return ONLY the category name for each hashtag
    - For substance-specific words, use the specific substance category
    - For consumption methods, prioritize "consumption_method" over substance categories
    - For platform-specific tags, use "platform"
    
    Format your response as:
    hashtag1: category1
    hashtag2: category2
    """
    
    examples = [
        {"role": "user", "content": "love, addiction, alcohol"},
        {"role": "assistant", "content": """
        love: emotions_and_feelings
        addiction: health_conditions
        alcohol: alcohol"""},
        {"role": "user", "content": "smoking, harmreduction, fyp"},
        {"role": "assistant", "content": """
        smoking: consumption_method
        harmreduction: awareness_and_advocacy
        fyp: platform"""}
    ]

    while retries > 0:
        try:
            messages = [
                {"role": "system", "content": prompt},
                *examples,
                {"role": "user", "content": f"Categorize these hashtags: {hashtags}"}
            ]
            
            response = client.chat.completions.create(
                messages=messages,
                model=model,
                temperature=0
            )
            
            return response.choices[0].message.content.strip()

        except Exception as e:
            print(f"An error occurred: {e}. Retries remaining: {retries-1}")
            retries -= 1
            time.sleep(2)

    return "skipped"