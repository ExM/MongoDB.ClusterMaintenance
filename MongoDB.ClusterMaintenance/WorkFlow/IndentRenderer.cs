namespace MongoDB.ClusterMaintenance.WorkFlow
{
	public static class IndentRenderer
	{
		public static string ToIndent(this int indent)
		{
			return indent == 0
				? ""
				: new string('-', indent) + " ";
		} 
	}
}