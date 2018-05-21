using System.IO;

namespace SnowMaker
{
    public class DebugOnlyFileDataStore : IOptimisticDataStore
    {
        private const string SeedValue = "1";

        private readonly string directoryPath;

        public DebugOnlyFileDataStore(string directoryPath)
        {
            this.directoryPath = directoryPath;
        }

        public string GetData(string blockName)
        {
            var blockPath = Path.Combine(directoryPath, $"{blockName}.txt");
            try
            {
                return File.ReadAllText(blockPath);
            }
            catch (FileNotFoundException)
            {
                using (var file = File.Create(blockPath))
                using (var streamWriter = new StreamWriter(file))
                {
                    streamWriter.Write(SeedValue);
                }
                return SeedValue;
            }
        }

        public bool TryOptimisticWrite(string blockName, string data)
        {
            var blockPath = Path.Combine(directoryPath, $"{blockName}.txt");
            File.WriteAllText(blockPath, data);
            return true;
        }
    }
}
