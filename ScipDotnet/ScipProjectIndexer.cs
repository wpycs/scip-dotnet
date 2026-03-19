using System.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Elfie.Extensions;
using Microsoft.CodeAnalysis.MSBuild;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ScipDotnet;

/// <summary>
/// Orchestrates Roslyn and MSBuild APIs to SCIP index a given project.
/// </summary>
public class ScipProjectIndexer
{
    public ScipProjectIndexer(ILogger<ScipProjectIndexer> logger) =>
        Logger = logger;

    private ILogger<ScipProjectIndexer> Logger { get; }

    private void Restore(IndexCommandOptions options, FileInfo project)
    {
        var arguments = project.Extension.Equals(".sln") ? $"restore {project.FullName} /p:EnableWindowsTargeting=true" : "restore /p:EnableWindowsTargeting=true";
        if (options.NugetConfigPath != null)
        {
            arguments += $" --configfile \"{options.NugetConfigPath.FullName}\"";
        }
        var process = new Process()
        {
            StartInfo = new ProcessStartInfo()
            {
                WorkingDirectory = options.WorkingDirectory.FullName,
                FileName = "dotnet",
                Arguments = arguments
            }
        };
        options.Logger.LogInformation("$ dotnet {Arguments}", arguments);
        process.Start();
        if (!process.WaitForExit(options.DotnetRestoreTimeout))
        {
            Logger.LogWarning("Dotnet restore did not finish in {Time} milliseconds, the results of the indexing might be incorrect.", options.DotnetRestoreTimeout);
        }
    }

    public async IAsyncEnumerable<Scip.Document> IndexDocuments(IHost host, IndexCommandOptions options,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var indexedProjects = new HashSet<ProjectId>();
        foreach (var project in options.ProjectsFile)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await foreach (var document in IndexProject(host, options, project, indexedProjects, cancellationToken))
            {
                yield return document;
            }
        }
    }

    private async IAsyncEnumerable<Scip.Document> IndexProject(IHost host,
                                                               IndexCommandOptions options,
                                                               FileInfo rootProject,
                                                               HashSet<ProjectId> indexedProjects,
                                                               [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!options.SkipDotnetRestore)
        {
            Restore(options, rootProject);
        }

        var projects = (string.Equals(rootProject.Extension, ".csproj") || string.Equals(rootProject.Extension, ".vbproj")
            ? new[]
            {
                await host.Services.GetRequiredService<MSBuildWorkspace>()
                    .OpenProjectAsync(rootProject.FullName, cancellationToken: cancellationToken)
            }
            : (await host.Services.GetRequiredService<MSBuildWorkspace>()
                .OpenSolutionAsync(rootProject.FullName, cancellationToken: cancellationToken)).Projects).ToList();


        options.Logger.LogDebug($"Found {projects.Count()} projects");
        var projectsPerProjFile = projects.GroupBy(x => x.FilePath).ToList();
        var totalProjects = projectsPerProjFile.Count;
        var projectIndex = 0;
        var framework = $"net{Environment.Version.Major}.0";
        foreach (var projectGroup in projectsPerProjFile)
        {
            projectIndex++;

            // If the project was found by opening the solution, we need to find the project that matches the framework.
            // if we can' fall back to the first one. Without this, we will process the same document multiple times
            // once for each framework version being targeting and it leads to unpredictable results since the scip file
            // will contain the same document multiple times iwth different symbols.
            var project = projectGroup.FirstOrDefault(x => x.Name.Contains($"({framework})", StringComparison.OrdinalIgnoreCase)) ?? projectGroup.First();
            if (project.Language != "C#" && project.Language != "Visual Basic")
            {
                Logger.LogWarning(
                    "Skipping project {ProjectFilePath} because it has language {ProjectLanguage} and scip-dotnet currently only supports C# and Visual Basic.",
                    project.FilePath, project.Language);
                continue;
            }

            if (indexedProjects.Contains(project.Id))
            {
                continue;
            }

            indexedProjects.Add(project.Id);

            var projectStopwatch = Stopwatch.StartNew();
            options.Logger.LogInformation("Indexing project [{ProjectIndex}/{TotalProjects}]: {ProjectPath}",
                projectIndex, totalProjects, project.FilePath);

            var globals = new Dictionary<ISymbol, ScipSymbol>(SymbolEqualityComparer.Default);

            var documentCount = 0;
            options.Logger.LogDebug($"Found {project.Documents.Count()} documents in {projectGroup.Key}");
            foreach (var document in project.Documents)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (options.Matcher.Match(options.WorkingDirectory.FullName, document.FilePath).HasMatches)
                {
                    yield return await IndexDocument(document, options, globals, project.Language, cancellationToken);
                    documentCount++;
                }
                else
                {
                    options.Logger.LogDebug(
                        "Excluded file path '{FilePath}' because it did not match the provided --include and --exclude arguments",
                        document.FilePath);
                }
            }

            options.Logger.LogInformation("Completed project [{ProjectIndex}/{TotalProjects}]: {ProjectPath} ({DocumentCount} documents, {Elapsed})",
                projectIndex, totalProjects, project.FilePath, documentCount, projectStopwatch.Elapsed.ToFriendlyString());
        }
    }

    private async Task<Scip.Document> IndexDocument(Document document,
                                                    IndexCommandOptions options,
                                                    Dictionary<ISymbol, ScipSymbol> globals,
                                                    string language,
                                                    CancellationToken cancellationToken = default)
    {
        Scip.Document doc = new()
        {
            Language = language,
            RelativePath = document.FilePath == null
                ? null
                : Path.GetRelativePath(options.WorkingDirectory.FullName, document.FilePath)
        };
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);
        if (semanticModel == null)
        {
            Logger.LogWarning(
                "Skipping document {DocumentFilePath} because document.GetSemanticModelAsync() returned null",
                document.FilePath);
        }
        else
        {
            var symbolFormatter = new ScipDocumentIndexer(doc, options, globals);
            var root = await document.GetSyntaxRootAsync(cancellationToken);
            if (language == "C#")
            {
                var walker = new ScipCSharpSyntaxWalker(symbolFormatter, semanticModel);
                walker.Visit(root);
            }
            else if (language == "Visual Basic")
            {
                var walker = new ScipVisualBasicSyntaxWalker(symbolFormatter, semanticModel);
                walker.Visit(root);
            }
        }

        return doc;
    }
}