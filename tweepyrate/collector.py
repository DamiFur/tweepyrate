import time
import tweepy
import threading

class Fetcher:
    def __init__(self, apps, minutes, process_tweets, count):
        self.apps = apps
        self.minutes = minutes
        self.process_tweets = process_tweets
        self.lock = threading.Lock()
        self.current_app = 0
        self.count = count

    def fetch(self, query, isUser, collection_name):
        """
        Fetches new tweets

        Arguments:
        ----------

        app: tweepy App
        """

        print("Querying {}".format(query))
        self.lock.acquire()
        print("Lock aquired by {}".format(query))
        while True:
            try:
                if isUser:
                    search = self.apps[self.current_app].user_timeline
                    print("Searching for tweets of user {}".format(query["screen_name"]))
                else:
                    search = self.apps[self.current_app].search
                print("Using app {}".format(self.apps[self.current_app].name))
                new_tweets = [tweet for tweet in tweepy.Cursor(search, **query).items(self.count)]
                print("Got {} new tweets".format(len(list(new_tweets))))
                break
            except tweepy.TweepError as e:
                if e.response and e.response.status_code == 404:
                    raise ValueError("User was invalid")
                if self.current_app == len(self.apps) - 1:
                    self.current_app = 0
                    print('exception raised, waiting to continue')
                    print('(until:', dt.datetime.now()+dt.timedelta(minutes=self.minutes), ')')
                    time.sleep(self.minutes*60)
                else:
                    self.current_app += 1
                    continue        

        # new_tweets = app.search(**query)
        self.process_tweets(new_tweets, collection_name)
        self.lock.release()
        return new_tweets



class Collector(threading.Thread):
    def __init__(self, process_tweets, fetcher, minutes, **kwargs):
        self.process_tweets = process_tweets
        self.args = kwargs
        self.args["count"] = 1000
        self.minutes = minutes
        self.args["tweet_mode"] = "extended"
        self.args["include_rts"] = False
        self.args["tweet_mode"] = "extended"
        self.fetcher = fetcher

        threading.Thread.__init__(self)

    def get_query(self):
        query = self.args.copy()

        return query

    def fetch(self):
        query = self.get_query()
        return self.fetcher.fetch(query, False, query["q"])


    def wait(self):
        print("Collector is waiting for {} minutes".format(self.minutes))
        time.sleep(self.minutes * 60)

    def run(self):
        while True:
            try:
                self.fetch()
                self.wait()
            except Exception as e:
                print(e)
                continue


class NewTweetsCollector(Collector):
    """
    Objects of this class are in charge of looking for new tweets for a given
    query
    """
    def __init__(self, process_tweets, fetcher, minutes, since_id=None, **kwargs):
        """Constructor

        Arguments:
        ---------

        """
        super().__init__(process_tweets, fetcher, minutes, **kwargs)
        self.since_id = since_id

    def get_query(self):
        query = super().get_query()

        if self.since_id:
            query["since_id"] = self.since_id

        return query

    def fetch(self):
        """
        Fetches new tweets

        Arguments:
        ----------

        app: tweepy App
        """

        new_tweets = super().fetch()

        if len(new_tweets) > 0:
            print("{} new tweets".format(len(new_tweets)))
            self.since_id = max(tw.id for tw in new_tweets) + 1
        else:
            # Busy waiting
            msg = "Search exhausted!!! Sleeping for {} minutes".format(
                self.minutes)
            print(msg)
            time.sleep(self.minutes * 60)

        return new_tweets


class PastTweetsCollector(Collector):
    """
    Objects of this class are in charge of looking for new tweets for a given
    query
    """
    def __init__(self, process_tweets, fetcher, minutes, max_id=None, **kwargs):
        """Constructor

        Arguments:
        ---------

        """
        super().__init__(process_tweets, fetcher, minutes, **kwargs)
        self.max_id = max_id

    def get_query(self):
        query = super().get_query()

        if self.max_id:
            query["max_id"] = self.max_id

        return query

    def fetch(self):
        """
        Fetches new tweets

        Arguments:
        ----------

        app: tweepy App
        """
        new_tweets = super().fetch()

        if len(new_tweets) > 0:
            print("{} new tweets".format(len(new_tweets)))
            self.max_id = min(tw.id for tw in new_tweets) - 1
        else:
            raise StopIteration("No more tweets left!")

        return new_tweets


class ByUsersCollector(Collector):
    def __init__(self, process_tweets, fetcher, minutes, isPositive, users=[], limit_id=None, direction="all", **kwargs):
        """Constructor

        Arguments:
        ---------

        """
        super().__init__(process_tweets, fetcher, minutes, **kwargs)
        self.users = users
        self.limit_id = limit_id
        self.direction = direction
        self.isPositive = isPositive
        self.current_user = 0

    def get_query(self):
        query = super().get_query()

        if self.limit_id:
            if direction == "new":
                query["since_id"] = self.limit_id
            if direction == "past":
                query["max_id"] = self.limit_id

        query["screen_name"] = self.users[self.current_user]
        self.current_user = (self.current_user + 1) % len(self.users)
        return query

    def fetch(self):
        """
        Fetches new tweets

        Arguments:
        ----------

        app: tweepy App
        """
        query = self.get_query()
        stance = "-Positive" if self.isPositive else "-Negative"
        new_tweets = []
        try:
            new_tweets = self.fetcher.fetch(query, True, query["q"] + stance)
        except ValueError:
            self.users.remove(self.users[self.current_user])
            print(self.users)

        if len(new_tweets) > 0:
            print("{} new tweets".format(len(new_tweets)))
            if self.limit_id:
                if direction == "new":
                    self.limit_id = max(tw.id for tw in new_tweets) + 1
                if direction == "past":
                    self.max_id = min(tw.id for tw in new_tweets) - 1
        else:
            raise StopIteration("No more tweets left!")

        return new_tweets
