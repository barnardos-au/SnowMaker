using System.Collections.Generic;
using System.Net;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;

namespace SnowMaker
{
    public class BlobOptimisticDataStore : IOptimisticDataStore
    {
        private const string SeedValue = "1";

        private readonly CloudBlobContainer blobContainer;

        private readonly IDictionary<string, ICloudBlob> blobReferences;
        private readonly object blobReferencesLock = new object();

        public BlobOptimisticDataStore(CloudStorageAccount account, string containerName)
        {
            var blobClient = account.CreateCloudBlobClient();
            blobContainer = blobClient.GetContainerReference(containerName.ToLower());
            blobContainer.CreateIfNotExistsAsync().Wait();

            blobReferences = new Dictionary<string, ICloudBlob>();
        }

        public string GetData(string blockName)
        {
            var blobReference = GetBlobReference(blockName);
            using (var stream = new MemoryStream())
            {
                blobReference.DownloadToStreamAsync(stream).Wait();
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }

        public bool TryOptimisticWrite(string scopeName, string data)
        {
            var blobReference = GetBlobReference(scopeName);
            try
            {
                UploadText(
                    blobReference,
                    data,
                    AccessCondition.GenerateIfMatchCondition(blobReference.Properties.ETag));
            }
            catch (StorageException exc)
            {
                if (exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                    return false;

                throw;
            }
            return true;
        }

        private ICloudBlob GetBlobReference(string blockName)
        {
            return blobReferences.GetValue(
                blockName,
                blobReferencesLock,
                () => InitializeBlobReference(blockName));
        }

        private ICloudBlob InitializeBlobReference(string blockName)
        {
            var blobReference = blobContainer.GetBlockBlobReference(blockName);

            if (blobReference.ExistsAsync().Result)
                return blobReference;

            try
            {
                UploadText(blobReference, SeedValue, AccessCondition.GenerateIfNoneMatchCondition("*"));
            }
            catch (StorageException uploadException)
            {
                if (uploadException.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw;
            }

            return blobReference;
        }

        void UploadText(ICloudBlob blob, string text, AccessCondition accessCondition)
        {
            blob.Properties.ContentEncoding = "UTF-8";
            blob.Properties.ContentType = "text/plain";
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(text)))
            {
                blob.UploadFromStreamAsync(stream, accessCondition, new BlobRequestOptions(), new OperationContext()).Wait();
            }
        }
    }
}
