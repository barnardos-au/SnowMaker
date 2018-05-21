using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;
using SnowMaker;
using System.Text;
using System.IO;

namespace IntegrationTests
{
    [TestFixture]
    public class Azure : Scenarios<Azure.TestScope>
    {
        private readonly CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;

        protected override TestScope BuildTestScope()
        {
            return new TestScope(CloudStorageAccount.DevelopmentStorageAccount);
        }

        protected override IOptimisticDataStore BuildStore(TestScope scope)
        {
            return new BlobOptimisticDataStore(storageAccount, scope.ContainerName);
        }

        public class TestScope : ITestScope
        {
            private readonly CloudBlobClient blobClient;

            public TestScope(CloudStorageAccount account)
            {
                var ticks = DateTime.UtcNow.Ticks;
                IdScopeName = $"snowmakertest{ticks}";
                ContainerName = $"snowmakertest{ticks}";

                blobClient = account.CreateCloudBlobClient();
            }

            public string IdScopeName { get; }
            public string ContainerName { get; }

            public string ReadCurrentPersistedValue()
            {
                var blobContainer = blobClient.GetContainerReference(ContainerName);
                var blob = blobContainer.GetBlockBlobReference(IdScopeName);
                using (var stream = new MemoryStream())
                {
                    blob.DownloadToStreamAsync(stream).Wait();
                    return Encoding.UTF8.GetString(stream.ToArray());
                }
            }

            public void Dispose()
            {
                var blobContainer = blobClient.GetContainerReference(ContainerName);
                blobContainer.DeleteAsync();
            }
        }
    }
}
