using System;
using System.IO;
using NUnit.Framework;
using SnowMaker;

namespace IntegrationTests
{
    [TestFixture]
    public class File : Scenarios<File.TestScope>
    {
        protected override TestScope BuildTestScope()
        {
            return new TestScope();
        }

        protected override IOptimisticDataStore BuildStore(TestScope scope)
        {
            return new DebugOnlyFileDataStore(scope.DirectoryPath);
        }

        public class TestScope : ITestScope
        {
            public TestScope()
            {
                var ticks = DateTime.UtcNow.Ticks;
                IdScopeName = $"snowmakertest{ticks}";

                DirectoryPath = Path.Combine(Path.GetTempPath(), IdScopeName);
                Directory.CreateDirectory(DirectoryPath);
            }

            public string IdScopeName { get; }
            public string DirectoryPath { get; }

            public string ReadCurrentPersistedValue()
            {
                var filePath = Path.Combine(DirectoryPath, $"{IdScopeName}.txt");
                return System.IO.File.ReadAllText(filePath);
            }

            public void Dispose()
            {
                if (Directory.Exists(DirectoryPath))
                    Directory.Delete(DirectoryPath, true);
            }
        }
    }
}
