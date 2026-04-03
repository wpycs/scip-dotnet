using Microsoft.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Scip;
using Document = Scip.Document;

namespace ScipDotnet;

/// <summary>
/// Creates SCIP <code>Document</code> based on provided symbols.
/// </summary>
public class ScipDocumentIndexer
{
    private readonly Document _doc;
    private readonly IndexCommandOptions _options;
    private int _localCounter;
    private readonly Dictionary<ISymbol, ScipSymbol> _globals;
    private readonly Dictionary<ISymbol, ScipSymbol> _locals = new(SymbolEqualityComparer.Default);
    private readonly string _markdownCodeFenceLanguage;

    // Custom formatting options to render symbol documentation. Feel free to tweak these parameters.
    // The options were derived by multiple rounds of experimentation with the goal of striking a
    // balance between showing detailed/accurate information without using too verbose syntax.
    private readonly SymbolDisplayFormat _format = new(
        globalNamespaceStyle: SymbolDisplayGlobalNamespaceStyle.OmittedAsContaining,
        typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameOnly,
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
        localOptions: SymbolDisplayLocalOptions.IncludeType |
                      SymbolDisplayLocalOptions.IncludeRef |
                      SymbolDisplayLocalOptions.IncludeConstantValue,
        kindOptions: SymbolDisplayKindOptions.IncludeTypeKeyword |
                     SymbolDisplayKindOptions.IncludeMemberKeyword |
                     SymbolDisplayKindOptions.IncludeNamespaceKeyword,
        miscellaneousOptions: SymbolDisplayMiscellaneousOptions.AllowDefaultLiteral |
                              SymbolDisplayMiscellaneousOptions.UseSpecialTypes |
                              SymbolDisplayMiscellaneousOptions.UseAsterisksInMultiDimensionalArrays |
                              SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier |
                              SymbolDisplayMiscellaneousOptions.EscapeKeywordIdentifiers
    );

    public ScipDocumentIndexer(
        Document doc,
        IndexCommandOptions options,
        Dictionary<ISymbol, ScipSymbol> globals)
    {
        _doc = doc;
        _options = options;
        _globals = globals;
        _markdownCodeFenceLanguage = _doc.Language == "C#" ? "cs" : "vb";
    }

    private ScipSymbol CreateScipSymbol(ISymbol? sym)
    {
        if (sym == null)
        {
            return ScipSymbol.Empty;
        }

        var fromCache = _globals.GetValueOrDefault(sym, ScipSymbol.Empty);
        if (fromCache != ScipSymbol.Empty)
        {
            return fromCache;
        }

        if (IsLocalSymbol(sym))
        {
            return CreateLocalScipSymbol(sym);
        }

        var owner = sym.Kind == SymbolKind.Namespace
            ? (sym.Name == "" ? CreateScipPackageSymbol(sym) : CreateScipSymbol(sym.ContainingSymbol))
            : CreateScipSymbol(sym.ContainingSymbol);

        if (owner.IsLocal())
        {
            return CreateLocalScipSymbol(sym);
        }

        var result = ScipSymbol.Global(owner, new SymbolDescriptor
        {
            Name = sym.Name,
            Suffix = SymbolSuffix(sym),
            Disambiguator = MethodDisambiguator(sym)
        });
        _globals.TryAdd(sym, result);
        return result;
    }

    private ScipSymbol CreateLocalScipSymbol(ISymbol sym)
    {
        var local = _locals.GetValueOrDefault(sym, ScipSymbol.Empty);
        if (local != ScipSymbol.Empty)
        {
            return local;
        }

        var localResult = ScipSymbol.Local(_localCounter++);
        _locals.TryAdd(sym, localResult);
        return localResult;
    }

    private ScipSymbol CreateScipPackageSymbol(ISymbol sym)
    {
        if (sym.ContainingAssembly == null)
        {
            return ScipSymbol.IndexLocalPackage;
        }

        if (!_options.AllowGlobalSymbolDefinitions && sym.Locations.Any(location => location.IsInSource))
        {
            // Emit index-local symbols to avoid exporting public symbols into the global scope (all repos in the world).
            // We have no guarantee that a random csproj file from any random repository is publishing to NuGet.
            // Use the command-line flag --allow-global-symbol-definitions to disable this behavior.
            return ScipSymbol.IndexLocalPackage;
        }

        return ScipSymbol.Package(
            sym.ContainingAssembly.Identity.Name,
            sym.ContainingAssembly.Identity.Version.ToString());
    }

    private SymbolDescriptor.Types.Suffix SymbolSuffix(ISymbol sym)
    {
        switch (sym.Kind)
        {
            case SymbolKind.Namespace:
                return SymbolDescriptor.Types.Suffix.Package;
            case SymbolKind.NamedType:
            case SymbolKind.FunctionPointerType:
            case SymbolKind.ErrorType:
            case SymbolKind.PointerType:
            case SymbolKind.ArrayType:
            case SymbolKind.DynamicType:
            case SymbolKind.Alias:
            case SymbolKind.Event:
                return SymbolDescriptor.Types.Suffix.Type;
            case SymbolKind.Property:
            case SymbolKind.Field:
            case SymbolKind.Assembly:
            case SymbolKind.Label:
            case SymbolKind.NetModule:
            case SymbolKind.RangeVariable:
            case SymbolKind.Preprocessing:
            case SymbolKind.Discard:
                return SymbolDescriptor.Types.Suffix.Term;
            case SymbolKind.Method:
                return SymbolDescriptor.Types.Suffix.Method;
            case SymbolKind.Parameter:
                return SymbolDescriptor.Types.Suffix.Parameter;
            case SymbolKind.TypeParameter:
                return SymbolDescriptor.Types.Suffix.TypeParameter;
            case SymbolKind.Local:
                return SymbolDescriptor.Types.Suffix.Local;
            default:
                _options.Logger.LogWarning("unknown symbol kind {SymKind}", sym.Kind);
                return SymbolDescriptor.Types.Suffix.Meta;
        }
    }

    private static string MethodDisambiguator(ISymbol sym)
    {
        if (sym is not IMethodSymbol methodSymbol)
        {
            return "";
        }

        var overloadCount = 0;
        var currentMethodSignature = GetMethodSignature(methodSymbol);

        // First try to get overloads from ContainingType.GetMembers()
        var members = sym.ContainingType.GetMembers();
        var foundInMembers = false;

        foreach (var member in members)
        {
            if (member.Name.Equals(sym.Name) && member is IMethodSymbol)
            {
                if (member.Equals(sym, SymbolEqualityComparer.Default))
                {
                    foundInMembers = true;
                    break;
                }
                overloadCount++;
            }
        }

        if (foundInMembers)
        {
            return overloadCount == 0 ? "" : $"+{overloadCount}";
        }

        // If not found in members (e.g., for external symbols), use parameter count as fallback
        // This helps distinguish overloads even when GetMembers() doesn't return all of them
        var paramCount = methodSymbol.Parameters.Length;

        // For external symbols, we can't reliably determine the exact overload index,
        // but we can use parameter count as a heuristic
        // Return empty string for now to avoid incorrect disambiguation
        return "";
    }

    /// <summary>
    /// Gets a signature string for a method to help identify overloads.
    /// </summary>
    private static string GetMethodSignature(IMethodSymbol method)
    {
        var paramTypes = string.Join(",", method.Parameters.Select(p => p.Type.Name));
        return $"{method.Name}({paramTypes})";
    }

    private readonly string[] _isIgnoredRelationshipSymbol =
    {
        " System/Object#",
        " System/Enum#",
        " System/ValueType#",
    };

    // Returns true if this symbol should not be emitted as a SymbolInformation relationship symbol.
    // The reason we ignore these symbols is because they appear automatically for a large number of
    // symbols putting pressure on our backend to index the inverted index. It's not particularly useful anyways
    // to query all the implementations of something like System/Object#.
    private bool IsIgnoredRelationshipSymbol(string symbol) =>
        _isIgnoredRelationshipSymbol.Any(symbol.EndsWith);

    /// <summary>
    /// Checks if a symbol is from an external source (e.g., NuGet package).
    /// External symbols have no source locations.
    /// </summary>
    private static bool IsExternalSymbol(ISymbol symbol) =>
        symbol.Locations.All(loc => !loc.IsInSource);

    public void VisitOccurrence(ISymbol? symbol, Location location, bool isDefinition, Location? enclosingLocation = null)
    {
        if (symbol == null)
        {
            return;
        }

        var symbolRole = 0;
        if (isDefinition)
        {
            symbolRole |= (int)SymbolRole.Definition;
        }

        var scipSymbol = CreateScipSymbol(symbol).Value;
        var occurrence = new Occurrence
        {
            Symbol = scipSymbol,
            SymbolRoles = symbolRole
        };
        _doc.Occurrences.Add(occurrence);
        foreach (var range in LocationToRange(location))
        {
            occurrence.Range.Add(range);
        }

        if (enclosingLocation != null)
        {
            foreach (var range in LocationToRange(enclosingLocation))
            {
                occurrence.EnclosingRange.Add(range);
            }
        }

        // Emit SymbolInformation for definition occurrences or for external symbols (NuGet references)
        // External symbols need display_name populated so they can be properly displayed in search results
        var isExternal = IsExternalSymbol(symbol);
        if (!isDefinition && !isExternal) return;

        var info = new SymbolInformation { Symbol = scipSymbol };
        _doc.Symbols.Add(info);

        var symbolSignature = symbol.ToDisplayString(_format);
        if (symbolSignature.Length > 0)
        {
            info.Documentation.Add($"```{_markdownCodeFenceLanguage}\n{symbolSignature}\n```");
        }

        var symbolDocumentation = symbol.GetDocumentationCommentXml();
        if (symbolDocumentation?.Length > 0)
        {
            info.Documentation.Add(symbolDocumentation);
        }

        // Only emit relationships for definition occurrences
        if (!isDefinition) return;

        switch (symbol)
        {
            case INamedTypeSymbol namedTypeSymbol:
                {
                    var baseType = namedTypeSymbol.BaseType;
                    while (baseType != null)
                    {
                        var baseTypeSymbol = CreateScipSymbol(baseType).Value;
                        if (IsIgnoredRelationshipSymbol(baseTypeSymbol))
                        {
                            break;
                        }

                        info.Relationships.Add(new Relationship
                        {
                            Symbol = baseTypeSymbol,
                            IsImplementation = true
                        });
                        baseType = baseType.BaseType;
                    }

                    foreach (var interfaceSymbol in namedTypeSymbol.AllInterfaces)
                    {
                        var interfaceSymbolSymbol = CreateScipSymbol(interfaceSymbol).Value;
                        if (IsIgnoredRelationshipSymbol(interfaceSymbolSymbol))
                        {
                            continue;
                        }

                        info.Relationships.Add(new Relationship
                        {
                            Symbol = interfaceSymbolSymbol,
                            IsImplementation = true
                        });
                    }

                    break;
                }
            case IMethodSymbol methodSymbol:
                {
                    var overriddenMethod = methodSymbol.OverriddenMethod;
                    while (overriddenMethod != null)
                    {
                        info.Relationships.Add(new Relationship
                        {
                            Symbol = CreateScipSymbol(overriddenMethod).Value,
                            IsImplementation = true,
                            IsReference = true
                        });
                        overriddenMethod = overriddenMethod.OverriddenMethod;
                    }

                    foreach (var interfaceMethod in ScipDocumentIndexer.InterfaceImplementations(methodSymbol))
                    {
                        info.Relationships.Add(new Relationship
                        {
                            Symbol = CreateScipSymbol(interfaceMethod).Value,
                            IsImplementation = true,
                            IsReference = true
                        });
                    }

                    break;
                }
            case IPropertySymbol propertySymbol:
                {
                    var overriddenProperty = propertySymbol.OverriddenProperty;
                    while (overriddenProperty != null)
                    {
                        info.Relationships.Add(new Relationship
                        {
                            Symbol = CreateScipSymbol(overriddenProperty).Value,
                            IsImplementation = true,
                            IsReference = true
                        });
                        overriddenProperty = overriddenProperty.OverriddenProperty;
                    }

                    foreach (var interfaceProp in InterfaceMemberImplementations(propertySymbol))
                    {
                        info.Relationships.Add(new Relationship
                        {
                            Symbol = CreateScipSymbol(interfaceProp).Value,
                            IsImplementation = true,
                            IsReference = true
                        });
                    }

                    break;
                }
            case IEventSymbol eventSymbol:
                {
                    var overriddenEvent = eventSymbol.OverriddenEvent;
                    while (overriddenEvent != null)
                    {
                        info.Relationships.Add(new Relationship
                        {
                            Symbol = CreateScipSymbol(overriddenEvent).Value,
                            IsImplementation = true,
                            IsReference = true
                        });
                        overriddenEvent = overriddenEvent.OverriddenEvent;
                    }

                    foreach (var interfaceEvent in InterfaceMemberImplementations(eventSymbol))
                    {
                        info.Relationships.Add(new Relationship
                        {
                            Symbol = CreateScipSymbol(interfaceEvent).Value,
                            IsImplementation = true,
                            IsReference = true
                        });
                    }

                    break;
                }
        }
    }

    // Returns explicitly and implicitly implemented interface methods by the given symbol method.
    // The Roslyn API has a `ExplicitInterfaceImplementations` that does not return implicitly implemented
    // methods.
    private static IEnumerable<ISymbol> InterfaceImplementations(IMethodSymbol symbol)
    {
        foreach (var interfaceSymbol in symbol.ContainingType.AllInterfaces)
        {
            foreach (var interfaceMember in interfaceSymbol.GetMembers())
            {
                var implementation = symbol.ContainingType.FindImplementationForInterfaceMember(interfaceMember);
                if (implementation != null && symbol.Equals(implementation, SymbolEqualityComparer.Default))
                {
                    yield return interfaceMember;
                }
            }
        }
    }

    // Returns interface members (properties, events, etc.) implemented by the given symbol.
    private static IEnumerable<ISymbol> InterfaceMemberImplementations(ISymbol symbol)
    {
        if (symbol.ContainingType == null) yield break;
        foreach (var interfaceSymbol in symbol.ContainingType.AllInterfaces)
        {
            foreach (var interfaceMember in interfaceSymbol.GetMembers())
            {
                var implementation = symbol.ContainingType.FindImplementationForInterfaceMember(interfaceMember);
                if (implementation != null && symbol.Equals(implementation, SymbolEqualityComparer.Default))
                {
                    yield return interfaceMember;
                }
            }
        }
    }

    // Converts a Roslyn location into a SCIP range.
    private static IEnumerable<int> LocationToRange(Location location)
    {
        var span = location.GetMappedLineSpan();
        if (span.StartLinePosition.Line == span.EndLinePosition.Line)
        {
            return new[]
                {
                    span.StartLinePosition.Line,
                    span.StartLinePosition.Character,
                    span.EndLinePosition.Character
                };
        }

        return new[]
            {
                span.StartLinePosition.Line,
                span.StartLinePosition.Character,
                span.EndLinePosition.Line,
                span.EndLinePosition.Character
            };
    }

    private static bool IsLocalSymbol(ISymbol sym)
    {
        return sym.Kind == SymbolKind.Local ||
               sym.Kind == SymbolKind.RangeVariable ||
               sym.Kind == SymbolKind.TypeParameter ||
               sym is IMethodSymbol { MethodKind: MethodKind.LocalFunction } ||
               // Anonymous classes/methods have empty names and can not be accessed outside their file.
               // The "global namespace" (parent of all namespaces) also has an empty name and should not
               // be treated as a local variable.
               (sym.Name.Equals("") && sym.Kind != SymbolKind.Namespace);
    }
}
