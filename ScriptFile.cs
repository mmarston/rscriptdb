﻿//   Copyright © 2005-2016 Commerce Technologies, LLC
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

namespace Mercent.AWS.Redshift
{
	internal class ScriptFile
	{
		public ScriptFile(string fileName)
		{
			if(fileName == null)
				throw new ArgumentNullException("fileName");
			this.FileName = fileName;
			this.Command = @"\i '" + fileName.Replace('\\', '/') + '\'';
		}

		public ScriptFile(string fileName, string command)
		{
			this.FileName = fileName;
			this.Command = command;
		}

		public string FileName { get; private set; }
		public string Command { get; private set; }
	}
}
