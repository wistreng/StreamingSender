using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading.Tasks;
using System;
using LinqToTwitter;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using Newtonsoft.Json.Linq;

namespace EventHubSender
{
    
    /// <summary>
    /// This program is for sending messages, sometimes simulating
    /// </summary>
    class Program
    {
        

        static void Main(string[] args)
        {
            var auth = new SingleUserAuthorizer
            {
                CredentialStore = new SingleUserInMemoryCredentialStore
                {
                    ConsumerKey = "JnUHjRrrxCrg0qaWB0uiTGFY3",
                    ConsumerSecret = "F8Sz7Td8n863JgoYWMlVmG5Lm6FsShhNFF87NSTFjRZuGNOOt4",
                    AccessToken = "745112483883417600-GfMjeRZo1avPBFmq6rooPeccyb6MJaD",
                    AccessTokenSecret = "MsGYjmR8jC393eEZsiTSQRMu3rXuLIWo6nyOepDKRf2DY"
                }
            };
            var twitterCtx = new TwitterContext(auth);

            List<Task> tasks = new List<Task>();



            string EhConnectionString = "Endpoint=sb://streaming-hubs.servicebus.windows.net/;SharedAccessKeyName=tweethub4sas;SharedAccessKey=qCXcGMPlyBypmHGgt8vPCMSkLV25buyvhseHktLJVTs=;EntityPath=tweets";
            string EventHubName = "tweets";
            string trackwords = "AwareGroup, #AIDAY19, #AI";


            //string EhConnectionString1 = "Endpoint=sb://streaming-hubs.servicebus.windows.net/;SharedAccessKeyName=tweet1sap;SharedAccessKey=/C1C8NZutA8VDkskrISO2nRT6QBgARhGABqicUlbirE=;EntityPath=tweet1";
            //string EventHubName1 = "tweet1";
            //string trackwords1 = "#CHINA";
            

            StreamingTweetsFeed(twitterCtx, EhConnectionString, EventHubName, trackwords).GetAwaiter().GetResult();
            //GetTwitterMessage(twitterCtx).GetAwaiter().GetResult();
            //SearchTweets(twitterCtx).GetAwaiter().GetResult();

            Dictionary<string, int> rooms = new Dictionary<string, int>(){{ "MC", 30 },{ "RC", 40 }};

            //Timeslot.TimeSlotFeeder(rooms).GetAwaiter().GetResult();
        }
        private static async Task StreamingTweetsFeed(TwitterContext twitterCtx, string EhConnectionString, string EventHubName, string track)
        {
            int count = 0;
            await
                (
                from strm in twitterCtx.Streaming
                where strm.Type == StreamingType.Filter && strm.Track == track
                select strm
                )
                .StartAsync(async strm =>
                {
                    if (strm.EntityType != StreamEntityType.Unknown)
                    {
                        Console.WriteLine("Sent {0} messages", count);
                        Debug.WriteLine(debugInfo(strm.Content) + "\n");
                        await EventHubSender.SendMessagesToEventHub(strm.Content, EventHubName, EhConnectionString);
                        await EventHubSender.SendMessagesToEventHub(strm.Content, "tweet1", "Endpoint=sb://streaming-hubs.servicebus.windows.net/;SharedAccessKeyName=tweet1sap;SharedAccessKey=/C1C8NZutA8VDkskrISO2nRT6QBgARhGABqicUlbirE=;EntityPath=tweet1");
                        //if (count++ >= 1000)
                        //    strm.CloseStream();


                    }
                });
        }

        public static string debugInfo(string content)
        {
            dynamic jsonData = JObject.Parse(content);

            string tweetId = jsonData["id_str"];
            string text = jsonData["text"];
            string userId = jsonData["user"]["id_str"];
            string userScreenName = jsonData["user"]["screen_name"];
            string userName = jsonData["user"]["name"];
            string userLoca = jsonData["user"]["location"];
            int userFollowers = jsonData["user"]["followers_count"];
            int userFriendCount = jsonData["user"]["friends_count"];
            int userFavouritesCount = jsonData["user"]["favourites_count"];
            int userStatusCount = jsonData["user"]["statuses_count"];
            int retweetCount = jsonData["retweet_count"];

            string Const_TwitterDateTemplate = "ddd MMM dd HH:mm:ss +ffff yyyy";
            DateTime createdAt = DateTime.ParseExact((string)jsonData["created_at"], Const_TwitterDateTemplate, new System.Globalization.CultureInfo("en-US"));

            return (
                userName +
                text
                );
        }

        private static async Task GetTwitterMessage(TwitterContext twitterCtx)
        {
            ulong tweetId = 745112483883417600;

            var tweets =
                await
                (from tweet in twitterCtx.Status
                 where tweet.Type == StatusType.Retweets &&
                       tweet.ID == tweetId
                 select tweet)
                .ToListAsync();

            if (tweets != null)
                tweets.ForEach(tweet =>
                {
                    if (tweet != null && tweet.User != null)
                        Debug.WriteLine(
                            "@{0} {1} ({2})",
                            tweet.User.ScreenNameResponse,
                            tweet.Text,
                            tweet.RetweetCount);
                });
        }

        private static async Task SearchTweets(TwitterContext twitterCtx)
        {
            const int MaxSearchEntriesToReturn = 10;
            const int MaxTotalResults = 100;

            string searchTerm = "GuoWenGui";

            // oldest id you already have for this search term
            ulong sinceID = 1;

            // used after the first query to track current session
            ulong maxID;

            var combinedSearchResults = new List<Status>();

            List<Status> searchResponse =
                await
                (from search in twitterCtx.Search
                 where search.Type == SearchType.Search &&
                       search.Query == searchTerm &&
                       search.Count == MaxSearchEntriesToReturn &&
                       search.SinceID == sinceID &&
                       search.TweetMode == TweetMode.Extended
                 select search.Statuses)
                .SingleOrDefaultAsync();

            if (searchResponse != null)
            {
                combinedSearchResults.AddRange(searchResponse);
                ulong previousMaxID = ulong.MaxValue;
                do
                {
                    // one less than the newest id you've just queried
                    maxID = searchResponse.Min(status => status.StatusID) - 1;

                    Debug.Assert(maxID < previousMaxID);
                    previousMaxID = maxID;

                    searchResponse =
                        await
                        (from search in twitterCtx.Search
                         where search.Type == SearchType.Search &&
                               search.Query == searchTerm &&
                               search.Count == MaxSearchEntriesToReturn &&
                               search.MaxID == maxID &&
                               search.SinceID == sinceID &&
                               search.TweetMode == TweetMode.Extended
                         select search.Statuses)
                        .SingleOrDefaultAsync();

                    combinedSearchResults.AddRange(searchResponse);
                } while (searchResponse.Any() && combinedSearchResults.Count < MaxTotalResults);

                combinedSearchResults.ForEach(tweet =>
                    Debug.WriteLine(
                        "\n  User: {0} ({1})\n  Tweet: {2}",
                        tweet.User.ScreenNameResponse,
                        tweet.User.UserIDResponse,
                        tweet.Text ?? tweet.FullText));
            }
            else
            {
                Debug.WriteLine("No entries found.");
            }
        }
    }
}
