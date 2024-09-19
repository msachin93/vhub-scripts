import pandas as pd
import requests
import numpy as np
import time, random

class ig_account():
  def __init__(self, data):
    self.__dict__ = data


class Scraper():
    def __init__(self, id, user,iters=40):
        self.cookie = 'sessionid=' + id
        self.headers = {'cookie': self.cookie,
                        'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 105.0.0.11.118 (iPhone11,8; iOS 12_3_1; en_US; en-US; scale=2.00; 828x1792; 165586599)'
                        }
        self.user = user
        # self.url_prefix = "http://127.0.0.1:5000/vedasis/scraping"
        self.url_prefix = "https://vhub-admin-backend-bsng2qeg2a-em.a.run.app/vedasis/scraping"

        self.profile_pics = []
        self.iterations = iters
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def urls(self, id, username, n):
        if(n==1):
            url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
        elif(n==2):
            url=f"https://i.instagram.com/api/v1/users/{id}/info/"
        elif(n==3):
            url = f"https://i.instagram.com/api/v1/feed/user/{username}/username/?count=1000"
        elif(n==5):
            url = f"https://i.instagram.com/api/v1/friendships/{id}/following/?count=1999"
        req = requests.get(url, headers=self.headers)
        return(req.json())

    def post_url(self,id):
        url = f"https://i.instagram.com/api/v1/media/{id}/info/"
        req = requests.get(url, headers=self.headers)
        return (req.json())

    def get_user_from_id(self, account):
        url = f"https://i.instagram.com/api/v1/users/{account.id}/info/"
        res = self.session.get(url).json()['user']
        account.__dict__.update(res)

    def get_user_from_username(self,account):
        url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={account.username}"
        res = self.session.get(url).json()['data']['user']
        account.__dict__.update(res)
        self.get_user_from_id(account)

    def get_posts_from_username(self, account):
        url = f"https://i.instagram.com/api/v1/feed/user/{account.username}/username/?count=1000"
        account.posts = self.session.get(url).json()['items']

    def get_following_list(self, user):
        output = pd.DataFrame()
        max_id=""
        while (1):
            url = f"https://i.instagram.com/api/v1/friendships/{user}/following/?count=200"
            if max_id != "":
                url = url + "&max_id=" + max_id
            req = requests.get(url, headers=self.headers)
            temp = pd.DataFrame(req.json()['users'])
            output = pd.concat([output, temp])
            if ('next_max_id' in req.json().keys()):
                max_id = req.json()['next_max_id']
            else:
                break
            if (len(output) >= 1000):
                break
        return(output)


    def fetch_ig_following(self):
        self.counter = 0
        while (self.iterations > 0):
            if (self.counter > 5):
                break
            time.sleep(random.uniform(1, 4))
            print('fetching ids')
            url = self.url_prefix + '/fetch_userids_for_following/' + self.user
            users = requests.get(url).json()
            print(int(len(users)/2), ' ids ready to be scraped')
            if (len(users) == 0):
                break
            output = pd.DataFrame()
            n=0
            c=0
            for user in users:
                n+=1
                if (self.counter > 5):
                    break
                try:
                    df = self.get_following_list(user)
                    if (len(df)>50):
                        df['main_id'] = user
                        if n%2 ==1:
                            df['session_id'] = 'count-'
                            print(user)
                            c+=1
                        else:
                            df['session_id'] = ''
                        output = pd.concat([output, df])
                    else:
                        if n%2 ==1:
                            print(user, "data too small")
                    self.counter = 0
                except Exception as e:
                    if n%2==1:
                        print('error ', user, e)
                    self.counter = self.counter + 1

            url = self.url_prefix + '/push_igdata_following'
            output['person'] = self.user
            output['session_id'] = output['session_id'] + self.cookie
            output = output.fillna('')

            for _ in range(3):
                response = requests.post(url, json=output.to_dict('records'))
                if response.status_code == 200:
                    print('profiles_pushed', c)
                    break
                elif response.status_code == 503:
                    print('Server temporarily unavailable, retrying...')
                    time.sleep(5)
                else:
                    print(response,"storing it as csv file ")
                    output.to_csv(f"following_push_error{random.randint(0,9999999)}.csv")
                    self.counter=10
                    break

            self.iterations -= 1

    def fetch_ig_users(self):
        self.counter = 0
        while (self.iterations > 0):
            if (self.counter > 20):
                break
            url = self.url_prefix + '/fetch_usernames_signedup/' + self.user
            accounts = requests.get(url).json()
            #users=   [{'username':'shubhirajthakur','id':''},{'username':'vickey120878','id':''}]
            if (len(accounts) == 0):
                break
            print(int(len(accounts)/2),'ids ready to get scraped')
            user_details = []
            post_details = []
            self.profile_pics = []
            n=0
            c=0
            for x in accounts:
                n+=1
                if (self.counter > 20):
                    break
                time.sleep(random.uniform(1, 4))
                try:
                    if n%2==1:
                        print(x['username'], x['id'], x['cnt'])
                    a = ig_account({'id': x['id'], 'username': x['username']})
                    if (x['id']=="" or x['id'] is None):
                      self.get_user_from_username(a)
                    else:
                      self.get_user_from_id(a)
                    
                    if n%2==1:
                        a.session_id = 'count-'
                    else:
                        a.session_id = ''
                    user_details.append(a.__dict__.copy())
                    try:
                      self.check_profile_pic(a.id,a.hd_profile_pic_versions[0]['url'])
                    except:
                      self.check_profile_pic(a.id, a.profile_pic_url)
                    self.get_posts_from_username(a)
                    post_details = post_details + a.posts
                    self.counter = 0
                    if n%2==1:
                        c+=1
                except Exception as e:
                    if n%2==1:
                        print(e)
                    self.counter = self.counter + 1


            output = pd.DataFrame(post_details)
            output['view_count'] = ''
            output = output[['taken_at', 'id', 'pk', 'media_type', 'code',
                                'filter_type', 'like_and_view_counts_disabled',
                                'is_paid_partnership', 'comment_count', 'like_count', 'caption',
                                'has_audio', 'video_duration', 'view_count', 'play_count', 'product_type', 'location', 'usertags']]

            output['has_audio'] = output['has_audio'].fillna('').astype('str')
            output['view_count']  = output['view_count'].replace('', np.nan).fillna(0.0).astype('float')
            output['play_count']  = output['play_count'].replace('', np.nan).fillna(0.0).astype('float')
            output['video_duration'] = output['video_duration'].replace('', np.nan).astype('float').fillna(0.0)

            df = pd.DataFrame(user_details)
            if 'interop_messaging_user_fbid' not in df.columns:
                df['interop_messaging_user_fbid'] = ""
            df['session_id'] = df['session_id'] + self.cookie
            df = df.dropna(subset=['full_name', 'biography',"follower_count"])
            df = df.fillna('')
            user_details = df.to_dict('records')
            url = self.url_prefix + '/push_profile_data/' + self.user
            response = requests.post(url, json={
                'profiles': user_details,
                'posts': output.fillna(0).to_dict('records'),
                "profiles_pic_url": self.profile_pics
                })

            if response.status_code != 200:
                print(response)
                break
            else:
                print('Users data_pushed ', c)
                # print('Posts data_pushed ', len(output), response.json()['posts_pushed'])
                # print('Profile pics uploaded ', len(self.profile_pics))

            self.iterations -= 1

    def check_profile_pic(self,insta_id,insta_url):
        try:
            url = 'https://storage.googleapis.com/vedasis-images/ig/' \
                + str(insta_id)[:3] + '/' + str(insta_id) + '.jpg'
            response = requests.head(url)
            if response.status_code != 200:
                data = {
                    "pic_url":insta_url,
                    "id": str(insta_id)
                }
                self.profile_pics.append(data)
        except Exception as ex:
            print("profile check error!",str(ex))