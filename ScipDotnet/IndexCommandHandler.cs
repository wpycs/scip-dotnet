using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Microsoft.CodeAnalysis.Elfie.Extensions;
using Microsoft.CodeAnalysis.MSBuild;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Scip;

namespace ScipDotnet;

public static class IndexCommandHandler
{
    public static async Task<int> Process(
        IHost host,
        List<FileInfo> projects,
        string output,
        FileInfo workingDirectory,
        List<string> include,
        List<string> exclude,
        bool allowGlobalSymbolDefinitions,
        int dotnetRestoreTimeout,
        bool skipDotnetRestore,
        FileInfo? nugetConfigPath,
        string outputFormat,
        bool incremental
        )
    {
        var logger = host.Services.GetRequiredService<ILogger<IndexCommandOptions>>();
        var matcher = new Matcher();
        matcher.AddIncludePatterns(include.Count == 0 ? new[] { "**" } : include);
        matcher.AddExcludePatterns(exclude);

        var projectFiles = projects.Count > 0
            ? projects
            : FindSolutionOrProjectFile(workingDirectory, logger);
        if (!projectFiles.Any())
        {
            return 1;
        }

        var options = new IndexCommandOptions(
            workingDirectory,
            OutputFile(workingDirectory, output),
            projectFiles,
            logger,
            matcher,
            allowGlobalSymbolDefinitions,
            dotnetRestoreTimeout,
            skipDotnetRestore,
            nugetConfigPath
        );
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        var cancellationToken = lifetime.ApplicationStopping;
        try
        {
            if (string.Equals(outputFormat, "sqlite", StringComparison.OrdinalIgnoreCase))
            {
                var sqliteOutput = output.EndsWith(".db", StringComparison.OrdinalIgnoreCase)
                    ? OutputFile(workingDirectory, output)
                    : OutputFile(workingDirectory, Path.ChangeExtension(output, ".db"));
                var sqliteOptions = options with { Output = sqliteOutput };
                await SqliteIndex(host, sqliteOptions, incremental, cancellationToken);
            }
            else
            {
                await ScipIndex(host, options, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Indexing was cancelled.");
            return 1;
        }


        // Log msbuild workspace diagnostic information after the index command finishes
        // We log the MSBuild failures as error since they are often blocking issues
        // preventing indexing. However, we log msbuild warnings as debug since they
        // do not block indexing usually and are much noisier
        var workspaceLogger = host.Services.GetRequiredService<ILogger<MSBuildWorkspace>>();
        var workspaceService = host.Services.GetRequiredService<MSBuildWorkspace>();
        if (workspaceService.Diagnostics.Any())
        {
            var diagnosticGroups = workspaceService.Diagnostics
                .GroupBy(d => new { d.Kind, d.Message })
                .Select(g => new { g.Key.Kind, g.Key.Message, Count = g.Count() });
            foreach (var diagnostic in diagnosticGroups)
            {
                var message = $"{diagnostic.Kind}: {diagnostic.Message} (occurred {diagnostic.Count} times)";
                if (diagnostic.Kind == Microsoft.CodeAnalysis.WorkspaceDiagnosticKind.Failure)
                {
                    workspaceLogger.LogError(message);
                }
                else
                {
                    workspaceLogger.LogDebug(message);
                }
            }
        }

        return 0;
    }

    private static FileInfo OutputFile(FileInfo workingDirectory, string output) =>
        Path.IsPathRooted(output) ? new FileInfo(output) : new FileInfo(Path.Join(workingDirectory.FullName, output));

    private static async Task ScipIndex(IHost host, IndexCommandOptions options, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var indexer = host.Services.GetRequiredService<ScipProjectIndexer>();
        var metadata = new Metadata
        {
            ProjectRoot = new Uri(new Uri("file://"), options.WorkingDirectory.FullName).ToString(),
            ToolInfo = new ToolInfo
            {
                Name = "scip-dotnet",
                Version = "0.1.0-SNAPSHOT"
            },
            TextDocumentEncoding = TextEncoding.Utf8,
        };

        var documentCount = 0;
        using (var fileStream = File.Create(options.Output.FullName))
        {
            var codedOutput = new CodedOutputStream(fileStream, leaveOpen: true);

            // Write metadata field (field 1, length-delimited)
            codedOutput.WriteTag(1, WireFormat.WireType.LengthDelimited);
            codedOutput.WriteMessage(metadata);
            codedOutput.Flush();

            // Stream each document directly to disk (field 2, length-delimited)
            await foreach (var document in indexer.IndexDocuments(host, options, cancellationToken: cancellationToken))
            {
                codedOutput.WriteTag(2, WireFormat.WireType.LengthDelimited);
                codedOutput.WriteMessage(document);
                codedOutput.Flush();
                documentCount++;
            }
        }

        if (documentCount <= 0)
        {
            options.Logger.LogWarning("Indexing finished without error but no documents were indexed.");
        }

        options.Logger.LogInformation("done: {OptionsOutput} {TimeElapsed}", options.Output,
            stopwatch.Elapsed.ToFriendlyString());
    }

    private static async Task SqliteIndex(IHost host, IndexCommandOptions options, bool incremental, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var indexer = host.Services.GetRequiredService<ScipProjectIndexer>();

        var isIncremental = incremental && File.Exists(options.Output.FullName);
        if (!isIncremental && File.Exists(options.Output.FullName))
            File.Delete(options.Output.FullName);

        if (isIncremental)
            options.Logger.LogInformation("Incremental mode: reusing existing index {Output}", options.Output);

        var documentCount = 0;
        var allVisitedPaths = new HashSet<string>();
        using (var writer = new SqliteIndexWriter(options.Output.FullName))
        {
            await foreach (var document in indexer.IndexDocuments(host, options, isIncremental ? writer : null, isIncremental ? allVisitedPaths : null, cancellationToken))
            {
                writer.WriteDocument(document);
                if (!string.IsNullOrEmpty(document.RelativePath))
                {
                    allVisitedPaths.Add(document.RelativePath);
                    // Store content hash for all runs so subsequent incremental runs
                    // can detect unchanged files.
                    var fullPath = Path.Combine(options.WorkingDirectory.FullName, document.RelativePath);
                    if (File.Exists(fullPath))
                    {
                        var content = await File.ReadAllTextAsync(fullPath, cancellationToken);
                        var hash = ComputeContentHash(content);
                        writer.UpdateContentHash(document.RelativePath, hash);
                    }
                }
                documentCount++;
            }

            if (isIncremental)
                writer.PurgeDeletedFiles(allVisitedPaths);

            if (documentCount <= 0 && writer.SkippedCount <= 0)
            {
                options.Logger.LogWarning("Indexing finished without error but no documents were indexed.");
            }

            writer.FinalizeIndex();
        }

        if (isIncremental)
        {
            options.Logger.LogInformation("done (sqlite, incremental): {OptionsOutput} ({DocumentCount} re-indexed, {SkippedCount} unchanged, {TimeElapsed})",
                options.Output, documentCount, allVisitedPaths.Count - documentCount, stopwatch.Elapsed.ToFriendlyString());
        }
        else
        {
            options.Logger.LogInformation("done (sqlite): {OptionsOutput} ({DocumentCount} documents, {TimeElapsed})",
                options.Output, documentCount, stopwatch.Elapsed.ToFriendlyString());
        }
    }

    private static string ComputeContentHash(string content)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(content));
        return Convert.ToHexString(bytes);
    }

    private static string FixThisProblem(string examplePath) =>
        "To fix this problem, pass the path of a solution (.sln) or project (.csproj/.vbrpoj) file to the `scip-dotnet index` command. " +
        $"For example, run: scip-dotnet index {examplePath}";

    private static List<FileInfo> FindSolutionOrProjectFile(FileInfo workingDirectory, ILogger logger)
    {
        var paths = Directory.GetFiles(workingDirectory.FullName).Where(file =>
            string.Equals(Path.GetExtension(file), ".sln", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(Path.GetExtension(file), ".csproj", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(Path.GetExtension(file), ".vbproj", StringComparison.OrdinalIgnoreCase)
        ).ToList();

        if (paths.Count != 0)
        {
            return paths.Select(path => new FileInfo(path)).ToList();
        }

        logger.LogError(
            "No solution (.sln) or .csproj/.vbproj file detected in the working directory '{WorkingDirectory}'. {FixThis}",
            workingDirectory.FullName, FixThisProblem("SOLUTION_FILE"));
        return new List<FileInfo>();
    }
}