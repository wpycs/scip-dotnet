using System.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Elfie.Extensions;
using Microsoft.Extensions.Logging;
using Scip;

namespace ScipDotnet;

/// <summary>
/// Indexes DLL assemblies using Roslyn metadata references to extract symbol information.
/// Unlike source-based indexing, this produces only SymbolInformation (no source locations).
/// </summary>
public class ScipAssemblyIndexer
{
    private readonly ILogger _logger;
    private readonly Dictionary<ISymbol, ScipSymbol> _globals = new(SymbolEqualityComparer.Default);

    private static readonly SymbolDisplayFormat DisplayFormat = new(
        globalNamespaceStyle: SymbolDisplayGlobalNamespaceStyle.OmittedAsContaining,
        typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypesAndNamespaces,
        genericsOptions: SymbolDisplayGenericsOptions.IncludeTypeParameters |
                         SymbolDisplayGenericsOptions.IncludeVariance |
                         SymbolDisplayGenericsOptions.IncludeTypeConstraints,
        memberOptions: SymbolDisplayMemberOptions.IncludeAccessibility |
                       SymbolDisplayMemberOptions.IncludeModifiers |
                       SymbolDisplayMemberOptions.IncludeParameters |
                       SymbolDisplayMemberOptions.IncludeRef |
                       SymbolDisplayMemberOptions.IncludeType |
                       SymbolDisplayMemberOptions.IncludeConstantValue |
                       SymbolDisplayMemberOptions.IncludeContainingType |
                       SymbolDisplayMemberOptions.IncludeExplicitInterface,
        delegateStyle: SymbolDisplayDelegateStyle.NameAndSignature,
        extensionMethodStyle: SymbolDisplayExtensionMethodStyle.InstanceMethod,
        parameterOptions: SymbolDisplayParameterOptions.IncludeType |
                          SymbolDisplayParameterOptions.IncludeName |
                          SymbolDisplayParameterOptions.IncludeDefaultValue |
                          SymbolDisplayParameterOptions.IncludeExtensionThis |
                          SymbolDisplayParameterOptions.IncludeOptionalBrackets |
                          SymbolDisplayParameterOptions.IncludeParamsRefOut,
        propertyStyle: SymbolDisplayPropertyStyle.ShowReadWriteDescriptor,
        kindOptions: SymbolDisplayKindOptions.IncludeTypeKeyword |
                     SymbolDisplayKindOptions.IncludeMemberKeyword |
                     SymbolDisplayKindOptions.IncludeNamespaceKeyword,
        miscellaneousOptions: SymbolDisplayMiscellaneousOptions.AllowDefaultLiteral |
                              SymbolDisplayMiscellaneousOptions.UseSpecialTypes |
                              SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier |
                              SymbolDisplayMiscellaneousOptions.EscapeKeywordIdentifiers
    );

    public ScipAssemblyIndexer(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Result of indexing a single DLL assembly.
    /// </summary>
    public record AssemblyIndexResult(string DllPath, List<SymbolInformation> Symbols);

    /// <summary>
    /// Creates a Roslyn compilation with all DLLs and search paths loaded as metadata references.
    /// </summary>
    public CSharpCompilation CreateCompilation(List<string> dllPaths, List<string> searchPaths)
    {
        var references = new List<MetadataReference>();

        foreach (var dllPath in dllPaths)
        {
            var fullPath = Path.GetFullPath(dllPath);
            if (!File.Exists(fullPath))
            {
                _logger.LogWarning("DLL not found: {Path}", fullPath);
                continue;
            }
            var xmlDocPath = Path.ChangeExtension(fullPath, ".xml");
            var docProvider = File.Exists(xmlDocPath)
                ? XmlDocumentationProvider.CreateFromFile(xmlDocPath)
                : null;
            references.Add(MetadataReference.CreateFromFile(fullPath, documentation: docProvider));
        }

        foreach (var searchPath in searchPaths)
        {
            var dir = Path.GetFullPath(searchPath);
            if (!Directory.Exists(dir)) continue;
            foreach (var dll in Directory.GetFiles(dir, "*.dll"))
            {
                if (dllPaths.Any(p => string.Equals(Path.GetFullPath(p), Path.GetFullPath(dll), StringComparison.OrdinalIgnoreCase)))
                    continue;
                references.Add(MetadataReference.CreateFromFile(dll));
            }
        }

        return CSharpCompilation.Create(
            "AssemblyIndexer",
            syntaxTrees: Array.Empty<SyntaxTree>(),
            references: references,
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
    }

    /// <summary>
    /// Indexes a single DLL from an existing compilation.
    /// </summary>
    public AssemblyIndexResult IndexSingleAssembly(CSharpCompilation compilation, string dllPath, bool includeNonPublic)
    {
        var fullPath = Path.GetFullPath(dllPath);
        var symbols = new List<SymbolInformation>();

        var reference = compilation.References
            .OfType<PortableExecutableReference>()
            .FirstOrDefault(r => string.Equals(r.FilePath, fullPath, StringComparison.OrdinalIgnoreCase));

        if (reference == null)
        {
            _logger.LogWarning("Reference not found in compilation for {Path}", fullPath);
            return new AssemblyIndexResult(fullPath, symbols);
        }

        var assemblySymbol = compilation.GetAssemblyOrModuleSymbol(reference) as IAssemblySymbol;
        if (assemblySymbol == null)
        {
            _logger.LogWarning("Could not resolve assembly symbol for {Path}", fullPath);
            return new AssemblyIndexResult(fullPath, symbols);
        }

        _logger.LogInformation("Indexing assembly: {Name} v{Version}",
            assemblySymbol.Identity.Name, assemblySymbol.Identity.Version);

        symbols.AddRange(WalkNamespace(assemblySymbol.GlobalNamespace, includeNonPublic));
        return new AssemblyIndexResult(fullPath, symbols);
    }

    private IEnumerable<SymbolInformation> WalkNamespace(INamespaceSymbol ns, bool includeNonPublic)
    {
        foreach (var member in ns.GetMembers())
        {
            switch (member)
            {
                case INamespaceSymbol childNs:
                    foreach (var info in WalkNamespace(childNs, includeNonPublic))
                        yield return info;
                    break;
                case INamedTypeSymbol type:
                    foreach (var info in WalkType(type, includeNonPublic))
                        yield return info;
                    break;
            }
        }
    }

    private IEnumerable<SymbolInformation> WalkType(INamedTypeSymbol type, bool includeNonPublic)
    {
        if (!ShouldInclude(type, includeNonPublic)) yield break;

        var typeInfo = CreateSymbolInfo(type);
        if (typeInfo != null)
        {
            if (type.BaseType != null && !IsIgnoredBaseType(type.BaseType))
            {
                var baseSymbol = CreateScipSymbol(type.BaseType);
                if (baseSymbol != ScipSymbol.Empty)
                    typeInfo.Relationships.Add(new Relationship { Symbol = baseSymbol.Value, IsImplementation = true });
            }
            foreach (var iface in type.AllInterfaces)
            {
                var ifaceSymbol = CreateScipSymbol(iface);
                if (ifaceSymbol != ScipSymbol.Empty)
                    typeInfo.Relationships.Add(new Relationship { Symbol = ifaceSymbol.Value, IsImplementation = true });
            }
            yield return typeInfo;
        }

        foreach (var member in type.GetMembers())
        {
            if (!ShouldInclude(member, includeNonPublic)) continue;
            if (member is INamedTypeSymbol nestedType)
            {
                foreach (var info in WalkType(nestedType, includeNonPublic))
                    yield return info;
                continue;
            }
            if (member.IsImplicitlyDeclared) continue;
            var memberInfo = CreateSymbolInfo(member);
            if (memberInfo != null)
            {
                AddMemberOverrideRelationships(member, memberInfo);
                yield return memberInfo;
            }
        }
    }

    private static bool ShouldInclude(ISymbol symbol, bool includeNonPublic)
    {
        if (includeNonPublic) return true;
        return symbol.DeclaredAccessibility is
            Accessibility.Public or Accessibility.Protected or Accessibility.ProtectedOrInternal;
    }

    private static readonly string[] IgnoredBaseTypes =
        { "System.Object", "System.ValueType", "System.Enum", "System.Delegate", "System.MulticastDelegate" };

    private static bool IsIgnoredBaseType(INamedTypeSymbol type) =>
        IgnoredBaseTypes.Contains(type.ToDisplayString());

    private void AddMemberOverrideRelationships(ISymbol member, SymbolInformation memberInfo)
    {
        switch (member)
        {
            case IMethodSymbol methodSymbol:
                {
                    var overridden = methodSymbol.OverriddenMethod;
                    while (overridden != null)
                    {
                        var sym = CreateScipSymbol(overridden);
                        if (sym != ScipSymbol.Empty)
                            memberInfo.Relationships.Add(new Relationship { Symbol = sym.Value, IsImplementation = true, IsReference = true });
                        overridden = overridden.OverriddenMethod;
                    }
                    break;
                }
            case IPropertySymbol propertySymbol:
                {
                    var overridden = propertySymbol.OverriddenProperty;
                    while (overridden != null)
                    {
                        var sym = CreateScipSymbol(overridden);
                        if (sym != ScipSymbol.Empty)
                            memberInfo.Relationships.Add(new Relationship { Symbol = sym.Value, IsImplementation = true, IsReference = true });
                        overridden = overridden.OverriddenProperty;
                    }
                    break;
                }
            case IEventSymbol eventSymbol:
                {
                    var overridden = eventSymbol.OverriddenEvent;
                    while (overridden != null)
                    {
                        var sym = CreateScipSymbol(overridden);
                        if (sym != ScipSymbol.Empty)
                            memberInfo.Relationships.Add(new Relationship { Symbol = sym.Value, IsImplementation = true, IsReference = true });
                        overridden = overridden.OverriddenEvent;
                    }
                    break;
                }
        }
    }

    private SymbolInformation? CreateSymbolInfo(ISymbol symbol)
    {
        var scipSymbol = CreateScipSymbol(symbol);
        if (scipSymbol == ScipSymbol.Empty) return null;
        var info = new SymbolInformation { Symbol = scipSymbol.Value };
        var signature = symbol.ToDisplayString(DisplayFormat);
        if (signature.Length > 0)
            info.Documentation.Add($"```cs\n{signature}\n```");
        var xmlDoc = symbol.GetDocumentationCommentXml();
        if (!string.IsNullOrEmpty(xmlDoc))
            info.Documentation.Add(xmlDoc);
        return info;
    }

    private ScipSymbol CreateScipSymbol(ISymbol? symbol)
    {
        if (symbol == null) return ScipSymbol.Empty;
        var cached = _globals.GetValueOrDefault(symbol, ScipSymbol.Empty);
        if (cached != ScipSymbol.Empty) return cached;
        var owner = symbol.Kind == SymbolKind.Namespace
            ? (symbol.Name == "" ? CreatePackageSymbol(symbol) : CreateScipSymbol(symbol.ContainingSymbol))
            : CreateScipSymbol(symbol.ContainingSymbol);
        if (owner == ScipSymbol.Empty && symbol.Kind != SymbolKind.Namespace)
            return ScipSymbol.Empty;
        var result = ScipSymbol.Global(owner, new SymbolDescriptor
        {
            Name = symbol.Name,
            Suffix = GetSuffix(symbol),
            Disambiguator = GetDisambiguator(symbol)
        });
        _globals.TryAdd(symbol, result);
        return result;
    }

    private static ScipSymbol CreatePackageSymbol(ISymbol symbol)
    {
        if (symbol.ContainingAssembly == null) return ScipSymbol.Empty;
        return ScipSymbol.Package(
            symbol.ContainingAssembly.Identity.Name,
            symbol.ContainingAssembly.Identity.Version.ToString());
    }

    private static SymbolDescriptor.Types.Suffix GetSuffix(ISymbol symbol) =>
        symbol.Kind switch
        {
            SymbolKind.Namespace => SymbolDescriptor.Types.Suffix.Package,
            SymbolKind.NamedType or SymbolKind.ArrayType or SymbolKind.DynamicType
                or SymbolKind.Event => SymbolDescriptor.Types.Suffix.Type,
            SymbolKind.Method => SymbolDescriptor.Types.Suffix.Method,
            SymbolKind.Property or SymbolKind.Field => SymbolDescriptor.Types.Suffix.Term,
            SymbolKind.Parameter => SymbolDescriptor.Types.Suffix.Parameter,
            SymbolKind.TypeParameter => SymbolDescriptor.Types.Suffix.TypeParameter,
            _ => SymbolDescriptor.Types.Suffix.Term
        };

    private static string GetDisambiguator(ISymbol symbol)
    {
        if (symbol is not IMethodSymbol) return "";
        var overloadIndex = 0;
        foreach (var member in symbol.ContainingType.GetMembers())
        {
            if (member.Name == symbol.Name && member is IMethodSymbol)
            {
                if (SymbolEqualityComparer.Default.Equals(member, symbol))
                    return overloadIndex == 0 ? "" : $"+{overloadIndex}";
                overloadIndex++;
            }
        }
        return "";
    }
}
