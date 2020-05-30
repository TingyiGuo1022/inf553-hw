import tweepy
import json
import sys
import random
import csv
import os
import collections

class Listener(tweepy.StreamListener):
    cur_num = 1
    stored = collections.defaultdict(list)
    tag_dict = collections.defaultdict(int)
    def __init__(self, output_file):
        self.output = output_file

    def on_data(self, data):
        
        message = json.loads(data)
        if "entities" not in message or "hashtags" not in message["entities"]: return
        cur_tags = message["entities"]["hashtags"]
        if len(cur_tags) == 0:
            return
        update_sample = (random.random() <= 100/self.cur_num)
        if update_sample:
            if self.cur_num > 100:
                delete_key = random.choice(list(self.stored.keys()))
                for tag in self.stored[delete_key]:
                    self.tag_dict[tag] -= 1
                del self.stored[delete_key]
        
            for tag in cur_tags:
                self.tag_dict[tag["text"]] += 1
                self.stored[self.cur_num].append(tag["text"])
        val_list = sorted(list(set(self.tag_dict.values())), reverse= True)[:3]
        output = sorted(self.tag_dict.items(), key = lambda x: x[0])
        output = sorted(output, key = lambda x: x[1], reverse= True)
        output = list(filter(lambda x: x[1] in val_list, output))
        
        file = open(self.output, "a")
        file.write("The number of tweets with tags from the beginning: "+str(self.cur_num)+"\n")
        for item in output:
            file.write(item[0]+" : "+str(item[1])+"\n")
        file.write("\n")
        self.cur_num += 1
        print(len(self.stored), self.cur_num-1)
        return
        
        

    def on_error(self, status_code):
        if status_code == 420:
            return False

if __name__ == "__main__":
    port = int(sys.argv[1])
    output_file = sys.argv[2]
    if os.path.exists(output_file): os.remove(output_file)
    consumer_key ="u5MB82Q3fQ3FrI0Slc6BMjUMy"
    consumer_secret = "2gjJP6ulf8LgOppyRceTvoB9gjCRCCy7W1KyrY5WitqaJvl03L"
    access_token = "1222065626220351488-uhQcwWCiaQQ9mIAgiNzCImWiLKLnDE"
    access_token_secret = "SmszavUWLlM9mkm8lULkE2xkYpii2MaEBit4mtPK3M73B"
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    cur_stream = tweepy.Stream(auth = api.auth, listener = Listener(output_file))
    cur_stream.filter(track=["nba"], locations=[-74.1687,40.5722,-73.8062,40.9467],languages=['en'])
    

    