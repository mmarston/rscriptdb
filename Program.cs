//   Copyright © 2005-2016 Commerce Technologies, LLC
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
using System;
using System.IO;
using System.Reflection;

namespace Mercent.AWS.Redshift
{
	class Program
	{
		static int Main(string[] args)
		{
			if(args.Length < 1)
			{
				ShowUsage();
				return 1;
			}
			else
			{
				return Create(args);
			}
		}

		static int Create(string[] args)
		{
			string connectionString = RedshiftUtility.UpdatePassword(args[0]);
			FileScripter scripter = new FileScripter(connectionString);
			for(int i = 1; i < args.Length; i++)
			{
				string arg = args[i];
				if(String.Equals(arg, "-f", StringComparison.OrdinalIgnoreCase))
				{
					if(scripter.ForceContinue == false)
					{
						Console.Error.WriteLine("Invalid arguments: -f cannot be combined with -n.");
						ShowUsage();
						return 1;
					}
					scripter.ForceContinue = true;
				}
				else if(String.Equals(arg, "-n", StringComparison.OrdinalIgnoreCase))
				{
					if(scripter.ForceContinue == true)
					{
						Console.Error.WriteLine("Invalid arguments: -n cannot be combined with -f.");
						ShowUsage();
						return 1;
					}
					scripter.ForceContinue = false;
				}
				else if(String.IsNullOrEmpty(scripter.OutputDirectory))
				{
					scripter.OutputDirectory = arg;
				}
				else
				{
					Console.Error.WriteLine("Unexpected argument: {0}", arg);
					ShowUsage();
					return 1;
				}
			}
			scripter.Script();
			return 0;
		}

		static void ShowUsage()
		{
			string program = Path.GetFileName(Assembly.GetExecutingAssembly().Location);
			Console.WriteLine
			(
				"Usage:\r\n"
					+ "\t{0} <ConnectionString> [<OutDirectory>] [-f|-n]\r\n",
				program
			);
		}
	}
}
