# Tweepyrate

This is a fork of the Tweepyrate library created by @finiteautomata for huge tweet retrieval of tweets using the open Twitter API

Features added by me are the following:
- A Fetcher that holds a list of apps and implements a round-robin, switching between apps when the one being used exceeds its quote limit.
- A Streaming collector for listening to twitter's live stream of tweets matching a query
- Multithreading for using multiple collectors at the same time and for storing the tweets while the fetcher keeps listening to new ones.
