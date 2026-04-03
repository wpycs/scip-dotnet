using Microsoft.Data.Sqlite;
using Scip;

namespace ScipDotnet.Tests;

[TestFixture]
public class SqliteInheritanceTests
{
    private string _dbPath = null!;
    private SqliteIndexWriter _writer = null!;

    [SetUp]
    public void SetUp()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_inheritance_{Guid.NewGuid():N}.db");
        _writer = new SqliteIndexWriter(_dbPath);
    }

    [TearDown]
    public void TearDown()
    {
        _writer.Dispose();
        // SQLite WAL mode may leave -wal and -shm files; give a brief delay then clean up
        SqliteConnection.ClearAllPools();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { /* best effort */ }
        try { if (File.Exists(_dbPath + "-wal")) File.Delete(_dbPath + "-wal"); } catch { }
        try { if (File.Exists(_dbPath + "-shm")) File.Delete(_dbPath + "-shm"); } catch { }
    }

    /// <summary>
    /// Helper: creates a SCIP Document with symbols that have inheritance relationships.
    /// </summary>
    private static Document CreateDocumentWithInheritance(
        string relativePath,
        (string symbol, string displayName, (string baseSymbol, bool isImpl)[] relationships)[] symbols)
    {
        var doc = new Document
        {
            Language = "csharp",
            RelativePath = relativePath
        };

        foreach (var (symbol, displayName, rels) in symbols)
        {
            var info = new SymbolInformation { Symbol = symbol };
            info.Documentation.Add(displayName);

            foreach (var (baseSymbol, isImpl) in rels)
            {
                info.Relationships.Add(new Relationship
                {
                    Symbol = baseSymbol,
                    IsImplementation = isImpl
                });
            }

            doc.Symbols.Add(info);

            // Add a definition occurrence so WriteDocument processes it
            var occ = new Occurrence { Symbol = symbol, SymbolRoles = (int)SymbolRole.Definition };
            occ.Range.Add(0); // startLine
            occ.Range.Add(0); // startChar
            occ.Range.Add(10); // endChar
            doc.Occurrences.Add(occ);
        }

        return doc;
    }

    [Test]
    public void DirectInheritance_IsStored()
    {
        // Animal <- Dog (Dog implements Animal)
        var doc = CreateDocumentWithInheritance("test.cs", new[]
        {
            ("scip-dotnet . . . Animal#", "Animal", Array.Empty<(string, bool)>()),
            ("scip-dotnet . . . Dog#", "Dog", new[] { ("scip-dotnet . . . Animal#", true) })
        });

        _writer.WriteDocument(doc);
        _writer.FinalizeIndex();

        var chains = QueryInheritanceChains(_dbPath);
        Assert.That(chains, Has.Count.GreaterThanOrEqualTo(1));
        Assert.That(chains, Has.Some.Matches<(string child, string baseSymbol, int depth)>(
            c => c.child.Contains("Dog") && c.baseSymbol.Contains("Animal") && c.depth == 1));
    }

    [Test]
    public void MultiLevelInheritance_IsStored()
    {
        // Animal <- Dog <- GuideDog
        var doc = CreateDocumentWithInheritance("test.cs", new[]
        {
            ("scip-dotnet . . . Animal#", "Animal", Array.Empty<(string, bool)>()),
            ("scip-dotnet . . . Dog#", "Dog", new[] { ("scip-dotnet . . . Animal#", true) }),
            ("scip-dotnet . . . GuideDog#", "GuideDog", new[] { ("scip-dotnet . . . Dog#", true) })
        });

        _writer.WriteDocument(doc);
        _writer.FinalizeIndex();

        var chains = QueryInheritanceChains(_dbPath);

        // Direct: GuideDog -> Dog (depth 1), Dog -> Animal (depth 1)
        Assert.That(chains, Has.Some.Matches<(string child, string baseSymbol, int depth)>(
            c => c.child.Contains("GuideDog") && c.baseSymbol.Contains("Dog") && c.depth == 1));
        Assert.That(chains, Has.Some.Matches<(string child, string baseSymbol, int depth)>(
            c => c.child.Contains("Dog") && c.baseSymbol.Contains("Animal") && c.depth == 1));
    }

    [Test]
    public void MultipleInterfaces_AreStored()
    {
        // Dog implements IAnimal and IRunnable
        var doc = CreateDocumentWithInheritance("test.cs", new[]
        {
            ("scip-dotnet . . . IAnimal#", "IAnimal", Array.Empty<(string, bool)>()),
            ("scip-dotnet . . . IRunnable#", "IRunnable", Array.Empty<(string, bool)>()),
            ("scip-dotnet . . . Dog#", "Dog", new[]
            {
                ("scip-dotnet . . . IAnimal#", true),
                ("scip-dotnet . . . IRunnable#", true)
            })
        });

        _writer.WriteDocument(doc);
        _writer.FinalizeIndex();

        var chains = QueryInheritanceChains(_dbPath);
        Assert.That(chains, Has.Some.Matches<(string child, string baseSymbol, int depth)>(
            c => c.child.Contains("Dog") && c.baseSymbol.Contains("IAnimal")));
        Assert.That(chains, Has.Some.Matches<(string child, string baseSymbol, int depth)>(
            c => c.child.Contains("Dog") && c.baseSymbol.Contains("IRunnable")));
    }

    [Test]
    public void NoInheritance_NoRows()
    {
        var doc = CreateDocumentWithInheritance("test.cs", new[]
        {
            ("scip-dotnet . . . Standalone#", "Standalone", Array.Empty<(string, bool)>())
        });

        _writer.WriteDocument(doc);
        _writer.FinalizeIndex();

        var chains = QueryInheritanceChains(_dbPath);
        Assert.That(chains, Is.Empty);
    }

    [Test]
    public void NonImplementationRelationship_IsIgnored()
    {
        // reference-only relationship should NOT appear in inheritance_chains
        var doc = new Document
        {
            Language = "csharp",
            RelativePath = "test.cs"
        };

        var info = new SymbolInformation { Symbol = "scip-dotnet . . . Foo#" };
        info.Documentation.Add("Foo");
        info.Relationships.Add(new Relationship
        {
            Symbol = "scip-dotnet . . . Bar#",
            IsReference = true,
            IsImplementation = false
        });
        doc.Symbols.Add(info);

        var occ = new Occurrence { Symbol = "scip-dotnet . . . Foo#", SymbolRoles = (int)SymbolRole.Definition };
        occ.Range.Add(0);
        occ.Range.Add(0);
        occ.Range.Add(10);
        doc.Occurrences.Add(occ);

        _writer.WriteDocument(doc);
        _writer.FinalizeIndex();

        var chains = QueryInheritanceChains(_dbPath);
        Assert.That(chains, Is.Empty);
    }

    [Test]
    public void ImplicitBaseTypes_AreIgnored()
    {
        // Dog implements System.Object — should be filtered out
        // MyEnum implements System.Enum, System.IFormattable, System.IComparable — all filtered
        var doc = CreateDocumentWithInheritance("test.cs", new[]
        {
            ("scip-dotnet nuget System.Runtime 8.0.0 System/Object#", "Object", Array.Empty<(string, bool)>()),
            ("scip-dotnet nuget System.Runtime 8.0.0 System/ValueType#", "ValueType", Array.Empty<(string, bool)>()),
            ("scip-dotnet nuget System.Runtime 8.0.0 System/Enum#", "Enum", Array.Empty<(string, bool)>()),
            ("scip-dotnet nuget System.Runtime 8.0.0 System/IFormattable#", "IFormattable", Array.Empty<(string, bool)>()),
            ("scip-dotnet nuget System.Runtime 8.0.0 System/IComparable#", "IComparable", Array.Empty<(string, bool)>()),
            ("scip-dotnet nuget System.Runtime 8.0.0 System/IConvertible#", "IConvertible", Array.Empty<(string, bool)>()),
            ("scip-dotnet . . . Dog#", "Dog", new[]
            {
                ("scip-dotnet nuget System.Runtime 8.0.0 System/Object#", true)
            }),
            ("scip-dotnet . . . MyEnum#", "MyEnum", new[]
            {
                ("scip-dotnet nuget System.Runtime 8.0.0 System/Enum#", true),
                ("scip-dotnet nuget System.Runtime 8.0.0 System/IFormattable#", true),
                ("scip-dotnet nuget System.Runtime 8.0.0 System/IComparable#", true),
                ("scip-dotnet nuget System.Runtime 8.0.0 System/IConvertible#", true)
            }),
            ("scip-dotnet . . . MyStruct#", "MyStruct", new[]
            {
                ("scip-dotnet nuget System.Runtime 8.0.0 System/ValueType#", true)
            })
        });

        _writer.WriteDocument(doc);
        _writer.FinalizeIndex();

        var chains = QueryInheritanceChains(_dbPath);
        Assert.That(chains, Is.Empty);
    }

    [Test]
    public void ExplicitBclInterface_IsKept()
    {
        // User explicitly implements IDisposable — should NOT be filtered
        var doc = CreateDocumentWithInheritance("test.cs", new[]
        {
            ("scip-dotnet nuget System.Runtime 8.0.0 System/IDisposable#", "IDisposable", Array.Empty<(string, bool)>()),
            ("scip-dotnet . . . MyResource#", "MyResource", new[]
            {
                ("scip-dotnet nuget System.Runtime 8.0.0 System/IDisposable#", true)
            })
        });

        _writer.WriteDocument(doc);
        _writer.FinalizeIndex();

        var chains = QueryInheritanceChains(_dbPath);
        Assert.That(chains, Has.Count.EqualTo(1));
        Assert.That(chains, Has.Some.Matches<(string child, string baseSymbol, int depth)>(
            c => c.child.Contains("MyResource") && c.baseSymbol.Contains("IDisposable")));
    }

    /// <summary>
    /// Reads inheritance_chains table directly via SQL for test verification.
    /// </summary>
    private static List<(string child, string baseSymbol, int depth)> QueryInheritanceChains(string dbPath)
    {
        var results = new List<(string, string, int)>();
        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT cs.symbol, bs.symbol, ic.depth
            FROM inheritance_chains ic
            JOIN global_symbols cs ON cs.id = ic.symbol_id
            JOIN global_symbols bs ON bs.id = ic.base_symbol_id";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
        }
        return results;
    }
}
