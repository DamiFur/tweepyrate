import tweepy
import json
import time
from .collector import NewTweetsCollector, PastTweetsCollector


def create_app(**kwargs):
    auth = tweepy.OAuthHandler(
            consumer_key=kwargs['consumer_key'],
            consumer_secret=kwargs['consumer_secret'],
    )
    auth.set_access_token(kwargs['access_token'], kwargs['access_secret'])

    app = tweepy.API(auth)

    app.name = kwargs['username']

    return app


def is_retweet(status):
    return hasattr(status, "retweeted_status")


def create_apps(json_file="config/my_apps.json"):
    with open(json_file) as f:
        app_keys = json.load(f)

    apps = [create_app(**app_key) for app_key in app_keys]
    print("{} apps created".format(len(apps)))
    return apps


class NoAppLeft(Exception):
    pass


def _handle_tweep_error(app, e):
    if e.api_code == 50:
        """User not found => reraise exception"""
        print("Reraising")
        raise e
    elif e.api_code == 63:
        """User suspended => reraise exception"""
        print("Reraising")
        raise e
    else:
        raise e


def call_for_each_app(apps, func):
    for app in apps:
        try:
            return func(app)
        except tweepy.RateLimitError as e:
            print("{} app exhausted".format(app.name))
            continue
        except tweepy.TweepError as e:
            print("{} app raised another error {}".format(app.name, e))
            _handle_tweep_error(app, e)

    raise NoAppLeft("No app left")


def fetch_tweets(apps, process_tweets, minutes=15, mode='new', **kwargs):
    """
    Fetches using Search API

    Arguments:

    apps: a list of tweepy api's

    process_tweets: callable
        Process batch of (tweets, query)

    minutes: int
        Minutes to sleep when exhausted or limit rate occurs.

    mode: "past" or "new", default: "new"
        Mode in which we should discover tweets: polling for new ones or
        diving into the past.
    """
    print("Search {} with params = {}".format(mode, kwargs))

    if mode == 'new':
        collector = NewTweetsCollector(
            process_tweets, minutes=minutes, **kwargs)
    elif mode == 'past':
        collector = PastTweetsCollector(
            process_tweets, minutes=minutes, **kwargs)
    else:
        raise ValueError("mode {} is not recognised, should be new or past".format(mode))

    print("Starting to collect...")
    while True:
        one_worked = False
        for app in apps:
            try:
                app_user = app.me()
                print("Trying app {}".format(app_user.screen_name))

                while True:
                    # Iteramos hasta que reviente esta app
                    new_tweets = collector.fetch(app)
                    one_worked = True

            except tweepy.TweepError as e:
                print("app {} - {}".format(app_user.screen_name, e))

        if not one_worked:
            print("Sleeping for {} minutes".format(minutes))
            print("="*80)
            print("="*80)

            time.sleep(60 * minutes)
        else:
            print("Trying again")
