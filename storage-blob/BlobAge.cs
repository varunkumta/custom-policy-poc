using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Storage.Blob
{
    public static class BlobAge
    {
        [FunctionName("BlobAge")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)]HttpRequest req, TraceWriter log)
        {
            log.Info("Starting to process blob age check");

            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            var token = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

            log.Info("Retrieved access token for storage");

            JToken requestJson = null;
            using (var reader = new StreamReader(req.Body))
            {
                var requestBodyString = await reader.ReadToEndAsync();

                if (!string.IsNullOrWhiteSpace(requestBodyString))
                {
                    requestJson = JToken.Parse(requestBodyString);
                }
            }

            if (requestJson == null || requestJson["storageBlobBaseUrl"] == null)
            {
                return new BadRequestObjectResult("Request body does not contain storageBlobBaseUrl");
            }

            log.Info("Parsed and validated request body");

            var storageBlobBaseUrl = requestJson["storageBlobBaseUrl"].ToString();

            var storageBlobClient = new CloudBlobClient(baseUri: new Uri(storageBlobBaseUrl), credentials: new StorageCredentials(new TokenCredential(token)));

            ContainerResultSegment containerSegment = null;
            BlobResultSegment blobSegment = null;
            var responseJson = new JArray();

            var config = new ConfigurationBuilder().AddEnvironmentVariables().Build();
            int maxAgeInDays = int.Parse(config["STORAGE_BLOB_MAXAGEINDAYS"]);

            do
            {
                containerSegment = await storageBlobClient.ListContainersSegmentedAsync(containerSegment != null && containerSegment.ContinuationToken != null ? containerSegment.ContinuationToken : null);

                foreach (var container in containerSegment.Results)
                {
                    log.Info($"Listing blobs in container {container.Name}");

                    do
                    {
                        blobSegment = await container.ListBlobsSegmentedAsync(
                            string.Empty,
                            true,
                            BlobListingDetails.None,
                            null,
                            blobSegment != null && blobSegment.ContinuationToken != null ? blobSegment.ContinuationToken : null,
                            new BlobRequestOptions(),
                            new OperationContext());

                        log.Info("Retrieved blob segment");

                        await ProcessBlobSegment(blobSegment.Results, storageBlobClient, maxAgeInDays, responseJson, log);

                    } while (blobSegment.ContinuationToken != null);
                }

            } while (containerSegment.ContinuationToken != null);

            return new OkObjectResult(responseJson);
        }

        private static async Task ProcessBlobSegment(IEnumerable<IListBlobItem> blobItems, CloudBlobClient storageBlobClient, int maxAgeInDays, JArray responseJson, TraceWriter log)
        {
            foreach (var blobItem in blobItems)
            {
                log.Info($"Retrieving blob and attributes for {blobItem.Uri.AbsoluteUri}");

                var blob = await storageBlobClient.GetBlobReferenceFromServerAsync(blobItem.Uri);
                await blob.FetchAttributesAsync();

                if (blob.Properties.Created.HasValue)
                {
                    if (blob.Properties.Created.Value < DateTimeOffset.UtcNow.AddDays(-1 * maxAgeInDays))
                    {
                        log.Info($"Adding non-compliant blob to response {blobItem.Uri.AbsoluteUri}");

                        responseJson.Add(JObject.Parse($"{{ \"blobUri\": \"{blobItem.Uri.AbsoluteUri}\", \"createdTime\": \"{blob.Properties.Created.Value.ToString("O")}\" }}"));
                    }
                }
                else
                {
                    log.Info("No created date");
                }
            }
        }
    }
}
