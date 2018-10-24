using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Rest.Azure;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

namespace KeyVault.Cert
{
    public static class CertExpiry
    {
        [FunctionName("CertExpiry")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)]HttpRequest req, TraceWriter log)
        {
            log.Info("Starting to process cert expiry check");

            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));

            log.Info("Initialized kv client");

            JToken requestJson = null;
            using (var reader = new StreamReader(req.Body))
            {
                var requestBodyString = await reader.ReadToEndAsync();

                if (!string.IsNullOrWhiteSpace(requestBodyString))
                {
                    requestJson = JToken.Parse(requestBodyString);
                }
            }

            if (requestJson == null || requestJson["vaultBaseUrl"] == null)
            {
                return new BadRequestObjectResult("Request body does not contain vaultBaseUrl");
            }

            log.Info("Parsed and validated request body");

            var vaultUrl = requestJson["vaultBaseUrl"].ToString();
            var responseJson = new JArray();

            var config = new ConfigurationBuilder().AddEnvironmentVariables().Build();
            int daysToExpiry = int.Parse(config["KEYVAULT_CERT_DAYSTOEXPIRY"]);

            var certItemsPage = await keyVaultClient.GetCertificatesAsync(vaultBaseUrl: vaultUrl);
            if (certItemsPage!= null && certItemsPage.Count() != 0)
            {
                log.Info($"Retrieved first cert page. Count:{certItemsPage.Count()}");

                await ProcessCertItemPage(certItemsPage, vaultUrl, keyVaultClient, log, responseJson, daysToExpiry);
            }

            while (certItemsPage != null && !string.IsNullOrWhiteSpace(certItemsPage.NextPageLink))
            {
                certItemsPage = await keyVaultClient.GetCertificatesNextAsync(nextPageLink: certItemsPage.NextPageLink);

                log.Info($"Retrieved next cert page. Count:{certItemsPage.Count()}");

                await ProcessCertItemPage(certItemsPage, vaultUrl, keyVaultClient, log, responseJson, daysToExpiry);
            }

            return new OkObjectResult(responseJson);
        }

        private static async Task ProcessCertItemPage(IPage<CertificateItem> certItemsPage, string vaultUrl, KeyVaultClient keyVaultClient, TraceWriter log, JArray responseJson, int daysToExpiry)
        {
            foreach (var certItem in certItemsPage)
            {
                var certBundle = await keyVaultClient.GetCertificateAsync(vaultBaseUrl: vaultUrl, certificateName: certItem.Identifier.Name);

                log.Info($"Retrieved cert {certItem.Id}");

                var x509Cert = new X509Certificate2(certBundle.Cer);
                if (x509Cert.NotAfter.ToUniversalTime() < DateTime.UtcNow.AddDays(daysToExpiry))
                {
                    log.Info($"Adding non-compliant cert to response {certItem.Id}");

                    responseJson.Add(JObject.Parse($"{{ \"certificateName\": \"{certItem.Identifier.Name}\", \"expiryTime\": \"{x509Cert.NotAfter.ToUniversalTime().ToString("O")}\" }}"));
                }
            }
        }
    }
}
