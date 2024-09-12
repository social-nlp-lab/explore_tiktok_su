# the script for labeling a post as durg-related or not
import pandas as pd
import json
import openai
import time
from threading import Lock

rate_limit = 10000
tpm_limit = 2000000  # Tokens per minute
rate_limit_period = 60  # seconds
retry_wait_time = 5  # seconds between retries

# Global variables for tracking rate limit
request_count = 0
token_count = 0
request_lock = Lock()
token_lock = Lock()

def get_theme(hashtags, retries=2, model=None, client=None):
    prompt = """
    Instruction: You are an expert linguist, specializing in content analysis related to substances and drug use. Your task is to semantically categorize phrases or hashtags from TikTok that have been associated with drug-related content. Here are the 17 defined categories:
    1- emotions and feelings: words and phrases related to emotional states and feelings. 
    2- health conditions: words and phrases related to health issues directly related to substance use, addiction-related health problems, or chronic conditions that may lead to substance use. Tags related to health issues stemming from substance use or conditions that might lead to substance use. 
    3- alcohol: words related to alcoholic beverages and spirits. This is just about the substances or drinks and not the side effects and consumption methods. 
    4- cannabis: substances related to marijuana, including recreational and medicinal use. 
    5- cognitive enhancement: substances about nootropics, smart drugs, and methods to improve cognitive function.
    6- commonly-misused substances: substances that are frequently used and misused such as licit and illicit substances. 
    7- consumption method: words and phrases that show specific ways in which substances (including alcohol, illicit or licit drugs including tobacco and cannabis) are ingested, administered, or used. 
    8- awareness and advocacy: words related to information and strategies to prevent and raise awareness of substance abuse or reduce associated harm, discussions about drug laws, policies, and related social issues, and content about addiction recovery, sobriety, and support systems.
    9- other substances: mentions of less common substances, oftentimes legal, over-the-counter medications, herbal remedies, or supplements.
    10- platform: tags and features specific to social media engagement, visibility, and trending tactics. 
    11- substance effects: words and phrases that describe the physical or mental effects of substance or alcohol use. This includes both desired effects and side effects. 
    12- tobacco_nicotine: words related to tobacco products, cigarettes, vaping, and nicotine use. 
    13- humor: words, phrases, or hashtags related to jokes, memes, or any content meant to be funny but specific to substance use, addiction, or recovery. 
    14- location: words related to geographcial locations, it could be city, state, country, or continent. 
    15- occupation: words related to occupations or professions. 
    16- identity-based risk groups: Hashtags related to any social identity, demographic group, or community affiliation that have faced or may currently experience marginalization, stigmatization, stereotyping, or labeling. This includes, but is not limited to, dimensions such as race, ethnicity, gender identity, sexual orientation, disability status, socioeconomic background, immigration status, religion, age group, or membership in specific subcultures.
    17- misc: Any tag that does not fit into the above categories. 
    Task: Categorize the hashtag provided below into exactly one of the 17 categories: cannabis, cognitive enhancement, platform, tobacco_nicotine, emotions and feelings, commonly-misused substances, other substances, substance effects, alcohol, consumption method, health conditions, awareness and advocacy, Identity-Based Risk Groups, humor, location, occupation, and misc.

    Notes: 
    Do not make new categories and only use the ones provided to you. 
    Some words may be misspelled, consider the correct spelling of the word when classifying but do not change the spellings of the hashtag or words in the result. 
    Do not give any explanations. 
    Slangs and euphemisms are present in this list, pointing to various types of illicit and licit drugs or using cannabis and other substances, for instance, drank, 40s, 30s (common substances), shmoke (consuming).  Take this into consideration when labeling the words. 
    Acronyms may be present in the set for instance NA representing Narcotics Anonymous.
    If the word is substance-specific, categorize it under the specific substance category. 
    If the word is a consumption method for instance drink, prioritize the "Consumption Method" category and not the substance or alcohol category. 

    Present your results in a clear, organized format, listing the categories and their respective hashtags.DO NOT CREATE NEW HASHTAGS. GROUP ONLY THE HASHTAGS PROVIDED AND GROUP ALL HASHTAGS.
    """
    examples = [
        {"role": "user", "content": "love, addiction, alcohol, cookies, foundersday,cannabis, modafinil, heroin, smoking, harmreduction, vitamins, fyp, stoned, vaping, addictionhumor, kensingtonphilly, nurselife, lgbtqia"},
        {"role": "assistant", "content": """
        {
            "emotions and feelings": ["love"],
            "health conditions": ["addiction"],
            "alcohol": ["alcohol"],
            "cannabis": ["cannabis"],
            "cognitive enhancement": ["modafinil"],
            "commonly-misused substances": ["heroin"],
            "consumption method": ["smoking"],
            "awareness and advocacy": ["harmreduction"],
            "other substances": ["vitamins"],
            "platform": ["fyp"],
            "substance effects": ["stoned"],
            "tobacco_nicotine": ["vaping"],
            "humor": ["addictionhumor"],
            "location": ["kensingtonphilly"],
            "occupation": ["nurselife"],
            "identity-based risk groups": ["lgbtqia"]
            "misc": ["cookies", "foundersday"]
        }
        """},
        {"role": "user", "content": "gratitude, chronicpain, vodka, stonersoftiktok, smartdrugs, xans, injection, soberlife, magnesium, viral, drunk, ecigs, drughumor, boston, medstudent, transgender"},
        {"role": "assistant", "content": """
        {
            "emotions and feelings": ["gratitude"],
            "health conditions": ["chronicpain"],
            "alcohol": ["vodka"],
            "cannabis": ["stonersoftiktok"],
            "cognitive enhancement": ["smartdrugs"],
            "commonly-misused substances": ["xans"],
            "consumption method": ["injection"],
            "awareness and advocacy": ["soberlife"],
            "other substances": ["magnesium"],
            "platform": ["viral"],
            "substance effects": ["drunk"],
            "tobacco_nicotine": ["ecigs"],
            "humor": ["drughumor"],
            "location": ["boston"],
            "occupation": ["medstudent"],
            "identity-based risk groups": ["transgender"]
        }
        """},
        {"role": "user", "content": "trauma, opioidaddiction, whiskey, weed, nootropics, fentfriday, drink, narcansaveslives, tylenol, tiktok, highasfuck, juul, recoveryhumor, philly, healthcareworkers, queergirl"},
        {"role": "assistant", "content": """
        {
            "emotions and feelings": ["trauma"],
            "health conditions": ["opioidaddiction"],
            "alcohol": ["whiskey"],
            "cannabis": ["weed"],
            "cognitive enhancement": ["nootropics"],
            "commonly-misused substances": ["fentfriday"],
            "consumption method": ["drink"],
            "awareness and advocacy": ["narcansaveslives"],
            "other substances": ["tylenol"],
            "platform": ["tiktok"],
            "substance effects": ["highasfuck"],
            "tobacco_nicotine": ["juul"],
            "humor": ["recoveryhumor"],
            "location": ["philly"],
            "occupation": ["healthcareworkers"],
            "identity-based risk groups": ["queergirl"]
        }
        """},
        {"role": "user", "content": "vibes, substanceusedisorder, cocktails, dabs, cerebrolysin, lean, shmoke, harmreductionworks, ibuprofen, trending, blackedout, cigs, addictionhumor, sanfrancisco, socialworker, whitegirl"},
        {"role": "assistant", "content": """
        {
            "emotions and feelings": ["vibes"],
            "health conditions": ["substanceusedisorder"],
            "alcohol": ["cocktails"],
            "cannabis": ["dabs"],
            "cognitive enhancement": ["cerebrolysin"],
            "commonly-misused substances": ["lean"],
            "consumption method": ["shmoke"],
            "awareness and advocacy": ["harmreductionworks"],
            "other substances": ["ibuprofen"],
            "platform": ["trending"],
            "substance effects": ["blackedout"],
            "tobacco_nicotine": ["cigs"],
            "humor": ["addictionhumor"],
            "location": ["sanfrancisco"],
            "occupation": ["socialworker"],
            "identity-based risk groups": ["whitegirl"]
        }
        """},
        {"role": "user", "content": "happytobealive, ptsd, tequila, 420vibes, modafinil, percs, inhaling, overdoseawareness, creatine, duet, hammered, nicotine, drughumor, usa, frontlineworkers, transtok"},
        {"role": "assistant", "content": """
        {
            "emotions and feelings": ["happytobealive"],
            "health conditions": ["ptsd"],
            "alcohol": ["tequila"],
            "cannabis": ["420vibes"],
            "cognitive enhancement": ["modafinil"],
            "commonly-misused substances": ["percs"],
            "consumption method": ["inhaling"],
            "awareness and advocacy": ["overdoseawareness"],
            "other substances": ["creatine"],
            "platform": ["duet"],
            "substance effects": ["hammered"],
            "tobacco_nicotine": ["nicotine"],
            "humor": ["drughumor"],
            "location": ["usa"],
            "occupation": ["frontlineworkers"],
            "identity-based risk groups": ["transtok"]
        }
        """}
    ]
    global request_count, token_count

    while retries > 0:
        try:
            with request_lock:
                if request_count >= rate_limit:
                    print("Rate limit reached. Pausing for a minute...")
                    time.sleep(rate_limit_period)
                    request_count = 0

            with token_lock:
                if token_count >= tpm_limit:
                    print("TPM limit reached. Pausing for a minute...")
                    time.sleep(rate_limit_period)
                    token_count = 0

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
            # response = client.chat.completions.create(
            #     messages=[
            #         {"role": "system", "content": prompt},
            #         {"role": "user", "content": example1},
            #         {"role": "system", "content": answer1},
            #         {"role": "user", "content": example2},
            #         {"role": "system", "content": answer2},
            #         {"role": "user", "content": example3},
            #         {"role": "system", "content": answer3},
            #         {"role": "user", "content": example4},
            #         {"role": "system", "content": answer4},
            #         {"role": "user", "content": example5},
            #         {"role": "system", "content": answer5},
            #         {"role": "user", "content": example6},
            #         {"role": "system", "content": answer6},
            #         {"role": "user", "content": example7},
            #         {"role": "system", "content": answer7},
            #         {"role": "user", "content": example8},
            #         {"role": "system", "content": answer8},
            #         {"role": "user", "content": example9},
            #         {"role": "system", "content": answer9},
            #         {"role": "user", "content": example10},
            #         {"role": "system", "content": answer10},
            #         {"role": "user", "content": hashtags}
            #     ],
            #     model=model,
            #     temperature=0
            # )

            with request_lock:
                request_count += 1

            with token_lock:
                token_count += sum([len(prompt), len(response.choices[0].message.content)])

            label = response.choices[0].message.content.lower().strip()
            return label

        except openai.RateLimitError as e:
            print(f"Rate limit error: {e}. Retrying in {retry_wait_time} seconds...")
            time.sleep(retry_wait_time)
            retries -= 1
        except Exception as e:
            print(f"An error occurred: {e}. Retrying...")
            retries -= 1
            time.sleep(retry_wait_time)

    print("Max retries reached. Skipping...")
    return "skipped"