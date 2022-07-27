# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 15:23:50 2022

@author: ETA SysNet
"""
import joblib
import pandas as pd
from google_play_scraper import Sort, reviews
# for building in wait times
import random
import time
import urllib.request

def runApp(app_id, sort_opt, prty, bnum):
    app_scraped_df = scrape(app_id, sort_opt, bnum)
    if type(app_scraped_df) == str:

        if app_scraped_df == 'error2':
            return 'Invalid App ID or No Review(s) For This App Yet'
        elif app_scraped_df == 'error1':
            return 'No Internet Network Detected'
    else:
        asd = clean_rvws(app_scraped_df)
        tp_catgry = classify_rvws(asd)
        filehand = sort_into_priority(tp_catgry, prty)
        return filehand

def scrape(app_id, sort_opt, bnum):
    # To store all the reviews scraped 
    total_revs_scraped = []
    # Print starting info for app
    print('---'*20)
    print('---'*20)    
    print(f'***** {app_id} started ')
    print()
    
    # Empty list for storing current reviews
    app_reviews = []
    
   
    # Number of reviews to scrape per batch
    count = 200
    
    # To keep track of how many batches have been completed
    batch_num = 0
    try:

        if sort_opt == 'Newest':
            
        # Retrieve reviews (and continuation_token) with reviews function
            rvws, token = reviews(
                app_id,           # found in app's url
                lang='en',        # defaults to 'en'
                country='us',     # defaults to 'us'
                sort=Sort.NEWEST, # start with most recent
                count=count       # batch size
            )
        else:
            rvws, token = reviews(
                app_id,           # found in app's url
                lang='en',        # defaults to 'en'
                country='us',     # defaults to 'us'
                sort=Sort.MOST_RELEVANT, # start with most recent
                count=count       # batch size
            )

    except urllib.error.URLError:

        return 'error1'

    if len(rvws) == 0:
        res = 'error2'
        return res
    
    for r in rvws:
        r['app_id'] = app_id     # add key for app's id
     
    
    # Add the list of review dicts to review list
    app_reviews.extend(rvws)
    
    # Increase batch count by one
    batch_num +=1 
    print(f'Batch {batch_num} completed. Other batches to begin soon')
    
    # Wait 1 to 5 seconds to start next batch
    time.sleep(random.randint(1,5))
    
    
    
    # Append review IDs to list prior to starting next batch
    # This will be used in the next phase for the purpose of checking for
    #repeated reviews in the next batches
    pre_review_ids = []
    for rvw in app_reviews:
        pre_review_ids.append(rvw['reviewId'])
    
    
    # Set max no of batches and Loop through
    for batch in range((bnum-1)):
        if sort_opt =='Newest':            
            rvws, token = reviews( # store continuation_token
                app_id,
                lang='en',
                country='us',
                sort=Sort.NEWEST,
                count=count,
                # using token obtained from previous batch
                continuation_token=token
            )
        else:
            rvws, token = reviews( # store continuation_token
                app_id,
                lang='en',
                country='us',
                sort=Sort.MOST_RELEVANT,
                count=count,
                # using token obtained from previous batch
                continuation_token=token
            )
        # Append unique review IDs from current batch to new list
        new_review_ids = []
        for r in rvws:
            new_review_ids.append(r['reviewId'])
            
            # And add app_id to each review dict
            r['app_id'] = app_id     # add key for app's id
     
        # Add the list of review dicts to main app_reviews list
        app_reviews.extend(rvws)
        
        # Increase batch count by one
        batch_num +=1
        print(f'\rBatch {batch_num} completed.', end='', flush=True)
        
        
        # Break loop and stop scraping for current app if most recent batch
          # did not add any unique reviews
        
        all_review_ids = pre_review_ids + new_review_ids
        if len(set(pre_review_ids)) == len(set(all_review_ids)):
            print(f'\n\n  No reviews left to scrape. Completed {batch_num} batches.')
            break
        
        # all_review_ids becomes pre_review_ids to check against 
          # for next batch
        pre_review_ids = all_review_ids
        
        # Store the reviews scraped so far in the main list
        #total_revs_scraped.extend(app_reviews)
               
    
    # Print update when max number of batches has been reached
      # OR when last batch didn't add any unique reviews
    print(f"  Done scraping '{app_id}'.\n")
    print(f"Scraped '{len(set(pre_review_ids))}' unique reviews for {app_id}\n")
    
    # Store the reviews scraped so far in the main list
    total_revs_scraped.extend(app_reviews)
    print(f'Scraped a total of {len(total_revs_scraped)} unique reviews so far.\n')
    
    
    
    app_scraped_df = pd.DataFrame(total_revs_scraped)
    
    # Print ending output for app
    print(f"""
    Successfully inserted all '{app_id}' reviews into Main list.\n
    """)
    print('---'*20)
    print('---'*20)
    print('\n')
   
    # Wait 1 to 5 seconds to start scraping next app
    time.sleep(random.randint(1,5))    
    return app_scraped_df

def clean_rvws(asd):
    
    asd = asd[asd.score != 4] #drop all >3 score
    asd = asd[asd.score != 5] #drop all >3 score    
    asd = asd.drop(columns = ['reviewId','userName', 'userImage', 'score', 'thumbsUpCount','reviewCreatedVersion','at','replyContent','repliedAt','app_id'])  #drop unwanted columns
    asd = asd.drop_duplicates(['content']) #remove duplicates from content
    asd['content'] = asd['content'].apply(lambda x: ' '.join([word for word in x.split() if word.isalnum()]))
    
    return asd

def classify_rvws(asd):
    import joblib
    # a dictionary of content and class will be formed. This will be returned for sort_into_category()
    #Create a category column in asd
    tp_catgry = {'Functional Error':[], 'Others':[], 'Bug Report':[], 'Question and Assistance':[],
                 'Feature Request (Removal or Enhancement)':[],
                 'User Interface or User Experience':[], 'App Usability':[],
                 'Hardware (Storage, Battery, Device)':[], 'Ethical Issues':[], 'Pricing':[],
                 'Security':[]}
    X = asd['content']
    
    tagger = joblib.load('svc.joblib3')
    tf = joblib.load('tf.joblib')
    
    for revws in X:        
        sent = tf.transform([revws])
        pred = tagger.predict(sent)
        for keys in tp_catgry.keys():
            if pred == keys:
                tp_catgry[keys].append(revws)
        
    return tp_catgry


def sort_into_priority(t_c, prty):
    Time = {'Pricing':[],'Question and Assistance':[],'Ethical Issues':[],'Security':[],
            'Feature Request (Removal or Enhancement)':[],
           'App Usability':[],'User Interface or User Experience':[],'Functional Error':[],
           'Bug Report':[], 'Hardware (Storage, Battery, Device)':[], 
           'Others':[], 'Time':[]}
    
    Cost = {'Hardware (Storage, Battery, Device)':[],'Feature Request (Removal or Enhancement)':[],
            'Bug Report':[],'Functional Error':[], 'App Usability':[],
           'User Interface or User Experience':[],'Security':[],'Ethical Issues':[], 
           'Question and Assistance':[], 'Pricing':[], 'Others':[], 'Cost':[]}
    
    Importance = {'App Usability':[], 'User Interface or User Experience':[],'Security':[], 
                  'Hardware (Storage, Battery, Device)':[], 'Bug Report':[], 'Functional Error':[],'Pricing':[],
                  'Feature Request (Removal or Enhancement)':[],'Ethical Issues':[],'Question and Assistance':[],
                  'Others':[], 'Importance':[]}
    
    if prty == 'Time':
        Time.update(t_c)
        # with open('file.txt', 'w+', encoding='utf8') as filehand:             
        #      print("REVIEWS CATEGORISED PER TIME ",sep='', end='\n\n', file=filehand, flush=False)
        #      for i in Time:
        #          print(i.upper(), sep='', end='\n- ', file=filehand, flush=False)
        #          print(*Time.get(i), sep='\n- ', end='\n\n', file=filehand, flush=False)
        return joblib.dump(Time, open('save.p', 'wb'))
    elif prty == 'Cost':
        Cost.update(t_c)
        # with open('file.txt', 'w+', encoding='utf8') as filehand:
        #      
        #      print("REVIEWS CATEGORISED PER COST ",sep='', end='\n\n', file=filehand, flush=False)
        #      for i in Cost:
        #          print(i.upper(), sep='', end='\n- ', file=filehand, flush=False)
        #          print(*Cost.get(i), sep='\n- ', end='\n\n', file=filehand, flush=False)
        return joblib.dump(Cost, open('save.p', 'wb'))
    else:
        Importance.update(t_c)
        # with open('file.txt', 'w+', encoding='utf8') as filehand:
        #      
        #      print("REVIEWS CATEGORISED PER IMPORTANCE ",sep='', end='\n\n', file=filehand, flush=False)
        #      for i in Importance:
        #          print(i.upper(), sep='', end='\n- ', file=filehand, flush=False)
        #          print('\n- '.join(Importance.get(i)), sep='', end='\n\n', file=filehand, flush=False)
        return joblib.dump(Importance, open('save.p', 'wb'))
    

def fileD(file):
    with open('file.txt', 'w+', encoding='utf8') as filehand:             
             print("REVIEWS CATEGORISED ",sep='', end='\n\n', file=filehand, flush=False)
             for i in file:
                 print(i.upper(), sep='', end='\n- ', file=filehand, flush=False)
                 print(*file.get(i), sep='\n- ', end='\n\n', file=filehand, flush=False)
    filehand.close()
    return filehand

    

#app = runApp('com.yahoo.mobile.client.android.mail', 'Newest', 'Cost', 7)