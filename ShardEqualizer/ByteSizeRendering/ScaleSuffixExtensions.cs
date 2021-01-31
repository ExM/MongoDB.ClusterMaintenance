using System;

namespace ShardEqualizer
{
	public static class ScaleSuffixExtensions
	{
		public static string Text(this ScaleSuffix scaleSuffix)
		{
			switch (scaleSuffix)
			{
				case ScaleSuffix.None: return "";
				case ScaleSuffix.Kilo: return "K";
				case ScaleSuffix.Mega: return "M";
				case ScaleSuffix.Giga: return "G";
				case ScaleSuffix.Tera: return "T";
				case ScaleSuffix.Peta: return "P";
				case ScaleSuffix.Exa: return "E";
				default:
					throw new ArgumentOutOfRangeException(nameof(scaleSuffix), scaleSuffix, null);
			}
		}
		
		public static long Factor(this ScaleSuffix scaleSuffix)
		{
			return (long)Math.Pow(2, (int) scaleSuffix);
		}
	}
}