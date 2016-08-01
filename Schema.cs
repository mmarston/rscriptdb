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

namespace Mercent.AWS.Redshift
{
	public class Schema
	{
		public Schema()
		{
			this.Tables = new TableCollection(this);
			this.Views = new ViewCollection(this);
		}

		public string AccessControlList { get; set; }
		public Database Database { get; internal set; }
		public string Description { get; set; }
		public string Name { get; set; }
		public string Owner { get; set; }
		public TableCollection Tables { get; private set; }
		public ViewCollection Views { get; private set; }

		public string GetQuotedName(QuoteMode mode = QuoteMode.WhenNecessary)
		{
			return RedshiftUtility.GetQuotedIdentifier(this.Name, mode);
		}

		public override string ToString()
		{
			return Name;
		}
	}
}
