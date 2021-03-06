import time
import tweepy
import threading
import datetime as dt



class StreamListenerAndStore(tweepy.StreamListener):
    def __init__(self, store_function, collection):
        super().__init__()
        self.store_function = store_function
        self.collection = collection
        self.stored = 0

    def on_status(self, status):
        self.store_function([status], "streaming", self.collection)
        self.stored += 1

    def on_error(self, status_code):
            if status_code == 420:
                print("Error 420 en el Listener")
                #returning False in on_data disconnects the stream
                return False
            else:
                print("Error con status code {}".format(status_code))

    def on_limit(self,status):
        print("Llegué al limite. Cierro el Stream")
        return False

    def on_disconnect(self, notice):
        print("Twitter te desconectó")
        return False

class Fetcher:
    def __init__(self, apps, stream_apps, minutes, process_tweets, count):
        self.apps = apps
        self.stream_apps = stream_apps
        self.minutes = minutes
        self.process_tweets = process_tweets
        self.lock = threading.Lock()
        self.current_app = 0
        self.count = count
        self.no_need_to_check = False

    def GetById(self, tweet_id):
        return self.apps[self.current_app].get_status(tweet_id, tweet_mode="extended")

    def fetch(self, query, isUser, collection_name):
        """
        Fetches new tweets

        Arguments:
        ----------

        app: tweepy App
        """

        #print("Querying {}".format(query))
        self.lock.acquire()
        print("Lock aquired by {}".format(query))
        while True:
            try:
                if isUser:
                    search = self.apps[self.current_app].user_timeline
                    print("Searching for tweets of user {}".format(query["screen_name"]))
                else:
                    search = self.apps[self.current_app].search
                if  "since_id" in query or "max_id" in query:
                    print("Api de search")
                    new_tweets = self.apps[self.current_app].search(**query)
                else:
                    print("Cursor")
                    new_tweets = [tweet for tweet in tweepy.Cursor(search, **query).items(self.count)]
                # print("Got {} new tweets".format(len(list(new_tweets))))
                break
            except tweepy.TweepError as e:
                if e.response and e.response.status_code == 404:
                    raise ValueError("User was invalid")
                if self.current_app == len(self.apps) - 1:
                    self.current_app = 0
                    print('exception raised: {}'.format(e))
                    print('(waiting to continue until:', dt.datetime.now()+dt.timedelta(minutes=self.minutes), ')')
                    time.sleep(self.minutes*60)
                else:
                    print('exception raised: {}'.format(e))
                    self.current_app += 1
                    continue

            except Exception as e:
                print("Hubo una excepción bajando tweets: {}".format(str(e)))
                break

        print("Guardando tweets")
        self.process_tweets(new_tweets, query['q'], collection_name, no_need_to_check=self.no_need_to_check)
        self.lock.release()
        return new_tweets

    def stream(self, queries, collection_name):
        while True:
            for app in self.stream_apps:
                print("Streaming con la app {}".format(app.name))
                try:
                    localStreamer = StreamListenerAndStore(self.process_tweets, collection_name)
                    stream = tweepy.Stream(auth=app.auth, listener=localStreamer)
                    print("Streaming for {}".format(queries))
                    stream.filter(track=queries)
                    #print("Bajamos {} tweets del streaming".format(stream.stored))
                except Exception as e:
                    print("Hubo una excepción stremeando con la app {}: {}".format(app.name, str(e)))
                    print("Bajamos {} tweets del streaming".format(stream.stored))
                    continue

            print("Streaming durmiendo por {} minutos".format(self.minutes))
            time.sleep(self.minutes * 60)



class Collector(threading.Thread):
    def __init__(self, collection, fetcher, minutes, **kwargs):
        self.args = kwargs
        self.args["count"] = 5000
        self.minutes = minutes
        self.collection = collection
        self.args["tweet_mode"] = "extended"
        self.fetcher = fetcher

        threading.Thread.__init__(self)

    def get_query(self):
        query = self.args.copy()

        return query

    def fetch(self):
        query = self.get_query()
        return self.fetcher.fetch(query, False, self.collection)


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
    def __init__(self, collection, fetcher, minutes, since_id=None, **kwargs):
        """Constructor

        Arguments:
        ---------

        """
        super().__init__(collection, fetcher, minutes, **kwargs)
        self.since_id = since_id
        self.no_need_to_check = True

    def get_query(self):
        query = super().get_query()

        query.pop("max_id", None)
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
            print("{} NEW tweets".format(len(new_tweets)))
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
    def __init__(self, collection, fetcher, minutes, max_id=None, **kwargs):
        """Constructor

        Arguments:
        ---------

        """
        super().__init__(collection, fetcher, minutes, **kwargs)
        self.max_id = max_id

    def get_query(self):
        query = super().get_query()

        query.pop("since_id", None)
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
            print("{} OLD tweets".format(len(new_tweets)))
            self.max_id = min(tw.id for tw in new_tweets) - 1
        else:
            raise StopIteration("No more tweets left!")

        return new_tweets


class StreamingCollector(Collector):
    def __init__(self, collection, queries, fetcher, minutes, **kwargs):
        super().__init__(collection, fetcher, minutes, **kwargs)
        self.queries = queries
        self.no_need_to_check = True

    def get_query(self):
        return super().get_query()

    def stream(self):
        self.fetcher.stream(self.queries, self.collection)
        print("Nunca debería llegar acá")

    def run(self):
        try:
            self.stream()
            self.wait()
        except Exception as e:
            print(e)



class ByUsersCollector(Collector):
    def __init__(self, collection, fetcher, minutes, isPositive, users=[], limit_id=None, direction="all", **kwargs):
        """Constructor

        Arguments:
        ---------

        """
        super().__init__(collection, fetcher, minutes, **kwargs)
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
        del query["include_rts"]
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
            new_tweets = self.fetcher.fetch(query, True, self.collection + stance)
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
