using System.Diagnostics;
using System.Security.Cryptography;
using Google.Protobuf;
using Microsoft.CodeAnalysis.Elfie.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Scip;

namespace ScipDotnet;

public static class IndexAssemblyCommandHandler
{
    public static Task<int> Process(
        IHost host,
        List<FileInfo> dllPaths,
        string output,
        string outputFormat,
        List<string> searchPaths,
        bool includeNonPublic,
        FileInfo? directory,
        bool incremental)
    {
        var logger = host.Services.GetRequiredService<ILogger<IndexCommandOptions>>();
        var stopwatch = Stopwatch.StartNew();

        var paths = dllPaths.Select(f => f.FullName).ToList();

        if (directory != null)
        {
            var dir = directory.FullName;
            if (!Directory.Exists(dir))
            {
                logger.LogError("Directory not found: {Path}", dir);
                return Task.FromResult(1);
            }
            var dirDlls = Directory.GetFiles(dir, "*.dll", SearchOption.AllDirectories);
            logger.LogInformation("Found {Count} DLL files in {Dir}", dirDlls.Length, dir);
            foreach (var dll in dirDlls)
            {
                if (!paths.Contains(dll, StringComparer.OrdinalIgnoreCase))
                    paths.Add(dll);
            }
        }

        if (paths.Count == 0)
        {
            logger.LogError("No DLL paths provided. Specify DLL files as arguments or use --directory.");
            return Task.FromResult(1);
        }

        var outputFile = Path.IsPathRooted(output)
            ? new FileInfo(output)
            : new FileInfo(Path.GetFullPath(output));

        if (string.Equals(outputFormat, "sqlite", StringComparison.OrdinalIgnoreCase))
        {
            var dbPath = output.EndsWith(".db", StringComparison.OrdinalIgnoreCase)
                ? outputFile
                : new FileInfo(Path.ChangeExtension(outputFile.FullName, ".db"));

            WriteSqlite(dbPath, paths, searchPaths, includeNonPublic, incremental, logger);
        }
        else
        {
            WriteScip(outputFile, paths, searchPaths, includeNonPublic, logger);
        }

        logger.LogInformation("done: {Output} ({Elapsed})", outputFile, stopwatch.Elapsed.ToFriendlyString());
        return Task.FromResult(0);
    }

    private static void WriteScip(FileInfo outputFile, List<string> dllPaths,
        List<string> searchPaths, bool includeNonPublic, ILogger logger)
    {
        var indexer = new ScipAssemblyIndexer(logger);
        var compilation = indexer.CreateCompilation(dllPaths, searchPaths);

        var allSymbols = new List<SymbolInformation>();
        foreach (var dllPath in dllPaths)
        {
            var result = indexer.IndexSingleAssembly(compilation, dllPath, includeNonPublic);
            allSymbols.AddRange(result.Symbols);
        }

        var metadata = new Metadata
        {
            ProjectRoot = "file://" + Path.GetDirectoryName(outputFile.FullName)?.Replace('\\', '/'),
            ToolInfo = new ToolInfo { Name = "scip-dotnet-ex", Version = "0.1.0-SNAPSHOT" },
            TextDocumentEncoding = TextEncoding.Utf8,
        };

        var doc = new Document { Language = "C#", RelativePath = "[assembly]" };
        foreach (var sym in allSymbols)
            doc.Symbols.Add(sym);

        using var fileStream = File.Create(outputFile.FullName);
        var codedOutput = new CodedOutputStream(fileStream, leaveOpen: true);
        codedOutput.WriteTag(1, WireFormat.WireType.LengthDelimited);
        codedOutput.WriteMessage(metadata);
        codedOutput.WriteTag(2, WireFormat.WireType.LengthDelimited);
        codedOutput.WriteMessage(doc);
        codedOutput.Flush();

        logger.LogInformation("Wrote SCIP index: {Path} ({Count} symbols)", outputFile.FullName, allSymbols.Count);
    }

    private static void WriteSqlite(FileInfo dbPath, List<string> dllPaths,
        List<string> searchPaths, bool includeNonPublic, bool incremental, ILogger logger)
    {
        var isIncremental = incremental && File.Exists(dbPath.FullName);
        if (!isIncremental && File.Exists(dbPath.FullName))
            File.Delete(dbPath.FullName);

        if (isIncremental)
            logger.LogInformation("Incremental mode: reusing existing index {Output}", dbPath);

        var indexer = new ScipAssemblyIndexer(logger);
        var compilation = indexer.CreateCompilation(dllPaths, searchPaths);

        var indexedCount = 0;
        var skippedCount = 0;
        var totalSymbols = 0;

        using var writer = new SqliteIndexWriter(dbPath.FullName);

        foreach (var dllPath in dllPaths)
        {
            var fullPath = Path.GetFullPath(dllPath);
            var fileHash = ComputeFileHash(fullPath);

            // Use DLL path as the document key for incremental tracking
            var docKey = "assembly:" + fullPath;

            if (isIncremental && !writer.ShouldReindex(docKey, fileHash))
            {
                skippedCount++;
                logger.LogDebug("Skipped (unchanged): {Path}", fullPath);
                continue;
            }

            // Purge old symbols from this DLL before re-indexing
            if (isIncremental)
            {
                writer.PurgeSymbolsBySource(docKey);
                writer.PurgeDocument(docKey);
            }

            var result = indexer.IndexSingleAssembly(compilation, dllPath, includeNonPublic);

            if (result.Symbols.Count > 0)
            {
                writer.WriteSymbols(result.Symbols, docKey);
                totalSymbols += result.Symbols.Count;
            }

            // Register the document and store its hash for future incremental runs
            writer.EnsureDocument("C#", docKey);
            writer.UpdateContentHash(docKey, fileHash);

            indexedCount++;
            logger.LogInformation("Indexed: {Path} ({Count} symbols)", fullPath, result.Symbols.Count);
        }

        writer.FinalizeIndex();

        logger.LogInformation("Assembly indexing complete: {Indexed} indexed, {Skipped} unchanged, {Symbols} total symbols",
            indexedCount, skippedCount, totalSymbols);
    }

    private static string ComputeFileHash(string filePath)
    {
        var bytes = File.ReadAllBytes(filePath);
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash);
    }
}
