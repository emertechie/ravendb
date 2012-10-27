using ICSharpCode.NRefactory.CSharp;
using ICSharpCode.NRefactory.PatternMatching;

namespace Raven.Database.Linq.Ast.QueryRewrite
{
	static class NRefactoryExtensions
	{
		public static T Detach<T>(this T node) where T : AstNode
		{
			node.Remove();
			return node;
		}

		public static Expression WithName(this Expression node, string patternGroupName)
		{
			return new NamedNode(patternGroupName, node);
		}
	}
}