using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.EventHubs;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using LinqToTwitter;
using System.Linq;
using System.Text;

namespace tweet.function
{
    public static class Function1
    {

        [FunctionName("Function1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            var auth = new SingleUserAuthorizer
            {
                CredentialStore = new SingleUserInMemoryCredentialStore
                {
                    ConsumerKey = GetEnvVar("ConsumerKey"),
                    ConsumerSecret = GetEnvVar("ConsumerSecret"),
                    AccessToken = GetEnvVar("AccessToken"),
                    AccessTokenSecret = GetEnvVar("AccessTokenSecret")
                }
            };
            var twitterCtx = new TwitterContext(auth);

            log.LogInformation("C# HTTP trigger function processed a request.");

            string trackword = req.Query["name"];
            string connectionString = req.Query["cntstr"];
            if (!Int32.TryParse(req.Query["count"], out int MESSAGE_COUNT))
                MESSAGE_COUNT = 1000;
            


            StreamingTweetsFeed(twitterCtx, trackword, connectionString, MESSAGE_COUNT).GetAwaiter().GetResult();

            //string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            //dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;
            //return name != null
            //    ? (ActionResult)new OkObjectResult($"Hello, {name}")
            //    : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
            return (ActionResult)new OkObjectResult($"{MESSAGE_COUNT} streaming tweets sent, track: {trackword}");
        }

        private static async Task StreamingTweetsFeed(TwitterContext twitterCtx, string trackwords, string connectionString, int MESSAGE_COUNT)
        {
            int count = 0;

            await (
                from stream in twitterCtx.Streaming
                where stream.Type == StreamingType.Filter && stream.Track == trackwords
                select stream
                ).StartAsync(async stream =>
                {
                    if(stream.EntityType != StreamEntityType.Unknown)
                    {
                        //send messages
                        Console.WriteLine("{0} messages send", count);
                        await SendMessageToEventHub(stream.Content, connectionString);
                        if(count++ >= MESSAGE_COUNT)
                            stream.CloseStream();
                    }
                });
        }

        private static async Task SendMessageToEventHub(string message, string connectionString)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString);

            EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            try
            {
                await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
            }

        }

        private static string GetEnvVar(string name)
        {
            return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
        }
    }
}
