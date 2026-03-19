using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.VisualBasic;
using Microsoft.CodeAnalysis.VisualBasic.Syntax;
using System.Diagnostics;

namespace ScipDotnet;

/// <summary>
/// Walks a single VisualBasic syntax tree and produces a SCIP <code>Document</code>.
/// </summary>
public class ScipVisualBasicSyntaxWalker : VisualBasicSyntaxWalker
{
    private readonly SemanticModel _semanticModel;
    private readonly ScipDocumentIndexer _scipDocumentIndexer;

    public ScipVisualBasicSyntaxWalker(ScipDocumentIndexer scipSymbolFormatter, SemanticModel semanticModel, SyntaxWalkerDepth depth = SyntaxWalkerDepth.Node) : base(depth)
    {
        _scipDocumentIndexer = scipSymbolFormatter;
        _semanticModel = semanticModel;
    }

    public override void VisitIdentifierName(IdentifierNameSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetSymbolInfo(node).Symbol, node.GetLocation(), false);
        base.VisitIdentifierName(node);
    }

    public override void VisitClassStatement(ClassStatementSyntax node)
    {
        // Parent is ClassBlockSyntax which covers the entire class
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitClassStatement(node);
    }

    public override void VisitModuleStatement(ModuleStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitModuleStatement(node);
    }

    public override void VisitEnumStatement(EnumStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitEnumStatement(node);
    }

    public override void VisitCatchStatement(CatchStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.IdentifierName.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitCatchStatement(node);
    }

    public override void VisitSubNewStatement(SubNewStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.NewKeyword.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitSubNewStatement(node);
    }

    public override void VisitDelegateStatement(DelegateStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.GetLocation());
        base.VisitDelegateStatement(node);
    }

    public override void VisitEventStatement(EventStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitEventStatement(node);
    }

    public override void VisitInterfaceStatement(InterfaceStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitInterfaceStatement(node);
    }

    public override void VisitPropertyStatement(PropertyStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitPropertyStatement(node);
    }

    public override void VisitStructureStatement(StructureStatementSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitStructureStatement(node);
    }

    public override void VisitVariableDeclarator(VariableDeclaratorSyntax node)
    {
        var enclosing = node.Parent?.GetLocation();
        foreach (var identifiers in node.Names)
        {
            _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(identifiers), identifiers.Identifier.GetLocation(), true, enclosing);
        }
        base.VisitVariableDeclarator(node);
    }

    public override void VisitEnumMemberDeclaration(EnumMemberDeclarationSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.GetLocation());
        base.VisitEnumMemberDeclaration(node);
    }

    public override void VisitMethodStatement(MethodStatementSyntax node)
    {
        // Parent is MethodBlockSyntax which covers the entire method
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.Parent?.GetLocation());
        base.VisitMethodStatement(node);
    }

    public override void VisitParameter(ParameterSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.GetLocation());
        base.VisitParameter(node);
    }

    public override void VisitTypeParameter(TypeParameterSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true);
        base.VisitTypeParameter(node);
    }

    public override void VisitExpressionRangeVariable(ExpressionRangeVariableSyntax node)
    {
        if (node.NameEquals != null)
        {
            _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node.NameEquals.Identifier), node.NameEquals.Identifier.GetLocation(), true, node.GetLocation());
        }
        base.VisitExpressionRangeVariable(node);
    }

    public override void VisitAggregationRangeVariable(AggregationRangeVariableSyntax node)
    {
        if (node.NameEquals != null)
        {
            _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node.NameEquals.Identifier), node.NameEquals.Identifier.GetLocation(), true, node.GetLocation());
        }
        base.VisitAggregationRangeVariable(node);
    }

    public override void VisitCollectionRangeVariable(CollectionRangeVariableSyntax node)
    {
        _scipDocumentIndexer.VisitOccurrence(_semanticModel.GetDeclaredSymbol(node), node.Identifier.GetLocation(), true, node.GetLocation());
        base.VisitCollectionRangeVariable(node);
    }
}
